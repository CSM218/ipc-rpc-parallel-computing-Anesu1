package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 *
 * CHALLENGE: You must handle 'Stragglers' (slow workers) and 'Partitions'
 * (disconnected workers).
 * A simple sequential loop will not pass the advanced autograder performance
 * checks.
 */
public class Master {

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
    private final Map<String, WorkerConnection> workers = new ConcurrentHashMap<>();
    private final Map<String, Task> inflightTasks = new ConcurrentHashMap<>();
    private final BlockingQueue<Task> pendingTasks = new LinkedBlockingQueue<>();
    private final AtomicInteger rrIndex = new AtomicInteger(0);
    private volatile ServerSocket serverSocket;

    private static final long HEARTBEAT_INTERVAL_MS = 1000;
    private static final long HEARTBEAT_TIMEOUT_MS = 4000;

    /**
     * Entry point for a distributed computation.
     *
     * Students must:
     * 1. Partition the problem into independent 'computational units'.
     * 2. Schedule units across a dynamic pool of workers.
     * 3. Handle result aggregation while maintaining thread safety.
     *
     * @param operation A string descriptor of the matrix operation (e.g.
     *                  "BLOCK_MULTIPLY")
     * @param data      The raw matrix data to be processed
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        if (data == null || data.length == 0) {
            return null;
        }

        // Simple example: parallel row sum for "SUM" operation.
        if ("SUM".equalsIgnoreCase(operation)) {
            ExecutorService pool = Executors.newFixedThreadPool(Math.max(1, workerCount));
            List<java.util.concurrent.Future<Integer>> futures = new ArrayList<>();
            for (int[] row : data) {
                futures.add(pool.submit(() -> {
                    int sum = 0;
                    for (int v : row) {
                        sum += v;
                    }
                    return sum;
                }));
            }
            int total = 0;
            for (java.util.concurrent.Future<Integer> f : futures) {
                try {
                    total += f.get();
                } catch (Exception e) {
                    // ignore and continue
                }
            }
            pool.shutdown();
            return total;
        }

        // Fallback: return null for unknown operations.
        return null;
    }

    /**
     * Start the communication listener.
     * Use your custom protocol designed in Message.java.
     */
    public void listen(int port) throws IOException {
        serverSocket = new ServerSocket(port);

        // Accept loop in background so listen() is non-blocking.
        systemThreads.submit(() -> {
            while (!serverSocket.isClosed()) {
                try {
                    Socket socket = serverSocket.accept();
                    handleConnection(socket);
                } catch (SocketException se) {
                    break;
                } catch (IOException e) {
                    // continue accepting
                }
            }
        });

        // Heartbeat scheduler for failure detection and timeout logic.
        heartbeatScheduler.scheduleAtFixedRate(this::reconcileState, 0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * System Health Check.
     * Detects dead workers and re-integrates recovered workers.
     */
    public void reconcileState() {
        long now = System.currentTimeMillis();

        for (WorkerConnection wc : new ArrayList<>(workers.values())) {
            if (now - wc.lastHeartbeat.get() > HEARTBEAT_TIMEOUT_MS) {
                // Worker timeout: reassign inflight tasks.
                wc.alive = false;
                workers.remove(wc.workerId);
                for (String taskId : wc.inflightTaskIds) {
                    Task task = inflightTasks.remove(taskId);
                    if (task != null) {
                        pendingTasks.offer(task);
                    }
                }
                closeQuietly(wc.socket);
            } else {
                // Send heartbeat ping
                sendHeartbeat(wc);
            }
        }

        // Try to dispatch any queued tasks.
        dispatchPending();
    }

    private void handleConnection(Socket socket) {
        systemThreads.submit(() -> {
            try (Socket s = socket;
                    DataInputStream in = new DataInputStream(s.getInputStream());
                    DataOutputStream out = new DataOutputStream(s.getOutputStream())) {

                while (!s.isClosed()) {
                    Message msg = readMessage(in);
                    if (msg == null) {
                        break;
                    }

                    if ("REGISTER_WORKER".equals(msg.messageType)) {
                        String workerId = new String(msg.payload, StandardCharsets.UTF_8);
                        WorkerConnection wc = new WorkerConnection(workerId, s, in, out);
                        workers.put(workerId, wc);
                        wc.lastHeartbeat.set(System.currentTimeMillis());

                        Message ack = new Message();
                        ack.messageType = "WORKER_ACK";
                        ack.studentId = msg.studentId;
                        ack.payload = "ACK".getBytes(StandardCharsets.UTF_8);
                        writeMessage(out, ack);
                    } else if ("HEARTBEAT".equals(msg.messageType)) {
                        String workerId = msg.studentId + ":" + socket.getRemoteSocketAddress();
                        WorkerConnection wc = workers.get(workerId);
                        if (wc != null) {
                            wc.lastHeartbeat.set(System.currentTimeMillis());
                        }
                    } else if ("RPC_REQUEST".equals(msg.messageType)) {
                        Task task = Task.fromRequest(msg, out);
                        pendingTasks.offer(task);
                        dispatchPending();
                    } else if ("TASK_COMPLETE".equals(msg.messageType)) {
                        handleTaskComplete(msg);
                    } else if ("TASK_ERROR".equals(msg.messageType)) {
                        handleTaskError(msg);
                    }
                }
            } catch (IOException e) {
                // connection failed, reconcile via heartbeat timeout
            }
        });
    }

    private void dispatchPending() {
        Task task;
        while ((task = pendingTasks.poll()) != null) {
            Optional<WorkerConnection> target = selectWorker();
            if (target.isEmpty()) {
                pendingTasks.offer(task);
                return;
            }
            WorkerConnection wc = target.get();
            inflightTasks.put(task.taskId, task);
            wc.inflightTaskIds.add(task.taskId);
            wc.inflightCount.incrementAndGet();
            task.workerId = wc.workerId;

            Message req = new Message();
            req.messageType = "RPC_REQUEST";
            req.studentId = task.studentId;
            req.payload = (task.taskId + ";" + task.taskType + ";" + task.payload)
                    .getBytes(StandardCharsets.UTF_8);
            writeMessage(wc.out, req);
        }
    }

    private Optional<WorkerConnection> selectWorker() {
        List<WorkerConnection> list = new ArrayList<>(workers.values());
        if (list.isEmpty()) {
            return Optional.empty();
        }
        int start = Math.abs(rrIndex.getAndIncrement());
        WorkerConnection best = null;
        for (int i = 0; i < list.size(); i++) {
            WorkerConnection wc = list.get((start + i) % list.size());
            if (!wc.alive) {
                continue;
            }
            if (best == null || wc.inflightCount.get() < best.inflightCount.get()) {
                best = wc;
            }
        }
        return Optional.ofNullable(best);
    }

    private void handleTaskComplete(Message msg) {
        String payload = new String(msg.payload, StandardCharsets.UTF_8);
        String[] parts = payload.split(";", 2);
        if (parts.length < 2) {
            return;
        }
        String taskId = parts[0];
        String result = parts[1];

        Task task = inflightTasks.remove(taskId);
        if (task != null) {
            WorkerConnection wc = workers.get(task.workerId);
            if (wc != null) {
                wc.inflightTaskIds.remove(taskId);
                wc.inflightCount.decrementAndGet();
            }
            // Send response back to requester
            Message resp = new Message();
            resp.messageType = "TASK_COMPLETE";
            resp.studentId = task.studentId;
            resp.payload = (taskId + ";" + result).getBytes(StandardCharsets.UTF_8);
            writeMessage(task.replyTo, resp);
        }
    }

    private void handleTaskError(Message msg) {
        String payload = new String(msg.payload, StandardCharsets.UTF_8);
        String[] parts = payload.split(";", 2);
        if (parts.length < 1) {
            return;
        }
        String taskId = parts[0];
        Task task = inflightTasks.remove(taskId);
        if (task != null) {
            pendingTasks.offer(task); // reassign on error
        }
    }

    private void sendHeartbeat(WorkerConnection wc) {
        Message hb = new Message();
        hb.messageType = "HEARTBEAT";
        hb.studentId = wc.workerId;
        hb.payload = "PING".getBytes(StandardCharsets.UTF_8);
        writeMessage(wc.out, hb);
    }

    private static void writeMessage(DataOutputStream out, Message msg) {
        try {
            byte[] packed = msg.pack();
            out.writeInt(packed.length);
            out.write(packed);
            out.flush();
        } catch (IOException e) {
            // ignore
        }
    }

    private static Message readMessage(DataInputStream in) throws IOException {
        int len = in.readInt();
        byte[] data = new byte[len];
        in.readFully(data);
        return Message.unpack(data);
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            // ignore
        }
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(getEnv("MASTER_PORT", "9999"));
        String studentId = getEnv("STUDENT_ID", "unknown");

        Master master = new Master();
        master.listen(port);
        System.out.println("Master started on port " + port + " for " + studentId);
    }

    private static String getEnv(String key, String def) {
        String val = System.getenv(key);
        return val == null || val.isEmpty() ? def : val;
    }

    private static class WorkerConnection {
        final String workerId;
        final Socket socket;
        final DataInputStream in;
        final DataOutputStream out;
        final AtomicLong lastHeartbeat = new AtomicLong(0);
        final AtomicInteger inflightCount = new AtomicInteger(0);
        final java.util.Set<String> inflightTaskIds = ConcurrentHashMap.newKeySet();
        volatile boolean alive = true;

        WorkerConnection(String workerId, Socket socket, DataInputStream in, DataOutputStream out) {
            this.workerId = workerId;
            this.socket = socket;
            this.in = in;
            this.out = out;
        }
    }

    private static class Task {
        final String taskId;
        final String taskType;
        final String payload;
        final String studentId;
        final DataOutputStream replyTo;
        String workerId;

        Task(String taskId, String taskType, String payload, String studentId, DataOutputStream replyTo) {
            this.taskId = taskId == null ? UUID.randomUUID().toString() : taskId;
            this.taskType = taskType;
            this.payload = payload;
            this.studentId = studentId;
            this.replyTo = replyTo;
        }

        static Task fromRequest(Message msg, DataOutputStream replyTo) {
            String raw = new String(msg.payload, StandardCharsets.UTF_8);
            String[] parts = raw.split(";", 3);
            String id = parts.length > 0 ? parts[0] : UUID.randomUUID().toString();
            String type = parts.length > 1 ? parts[1] : "MATRIX_MULTIPLY";
            String payload = parts.length > 2 ? parts[2] : "";
            return new Task(id, type, payload, msg.studentId, replyTo);
        }
    }
}
