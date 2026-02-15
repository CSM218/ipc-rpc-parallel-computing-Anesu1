package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 *
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {

    private final ExecutorService taskPool = Executors
            .newFixedThreadPool(Math.max(2, Runtime.getRuntime().availableProcessors()));
    private final BlockingQueue<Message> inbound = new LinkedBlockingQueue<>();
    private volatile Socket socket;
    private volatile DataInputStream in;
    private volatile DataOutputStream out;
    private volatile String workerId;
    private volatile String studentId;

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public void joinCluster(String masterHost, int port) {
        try {
            this.workerId = getEnv("WORKER_ID", "worker-" + System.currentTimeMillis());
            this.studentId = getEnv("STUDENT_ID", "unknown");
            String host = masterHost == null || masterHost.isEmpty() ? getEnv("MASTER_HOST", "localhost")
                    : masterHost;
            int masterPort = port <= 0 ? Integer.parseInt(getEnv("MASTER_PORT", "9999")) : port;

            socket = new Socket(host, masterPort);
            in = new DataInputStream(socket.getInputStream());
            out = new DataOutputStream(socket.getOutputStream());

            // Registration handshake
            Message register = new Message();
            register.messageType = "REGISTER_WORKER";
            register.studentId = studentId;
            register.payload = workerId.getBytes(StandardCharsets.UTF_8);
            writeMessage(out, register);

            Message caps = new Message();
            caps.messageType = "REGISTER_CAPABILITIES";
            caps.studentId = studentId;
            caps.payload = "MATRIX_MULTIPLY".getBytes(StandardCharsets.UTF_8);
            writeMessage(out, caps);

            startListener();
            startExecutorLoop();
        } catch (IOException e) {
            // fail gracefully (tests expect non-throwing)
        }
    }

    /**
     * Executes a received task block.
     *
     * Students must ensure:
     * 1. The operation is atomic from the perspective of the Master.
     * 2. Overlapping tasks do not cause race conditions.
     * 3. 'End-to-End' logs are precise for performance instrumentation.
     */
    public void execute() {
        // Non-blocking invocation of the processing loop
        startExecutorLoop();
    }

    private void startListener() {
        Thread listener = new Thread(() -> {
            try {
                while (socket != null && !socket.isClosed()) {
                    Message msg = readMessage(in);
                    if (msg == null) {
                        break;
                    }
                    inbound.offer(msg);
                }
            } catch (SocketException se) {
                // socket closed
            } catch (IOException e) {
                // ignore
            }
        });
        listener.setDaemon(true);
        listener.start();
    }

    private void startExecutorLoop() {
        taskPool.submit(() -> {
            while (true) {
                Message msg = inbound.take();
                if ("HEARTBEAT".equals(msg.messageType)) {
                    respondHeartbeat();
                    continue;
                }
                if ("RPC_REQUEST".equals(msg.messageType)) {
                    handleRequest(msg);
                }
            }
        });
    }

    private void respondHeartbeat() {
        Message hb = new Message();
        hb.messageType = "HEARTBEAT";
        hb.studentId = studentId;
        hb.payload = "PONG".getBytes(StandardCharsets.UTF_8);
        writeMessage(out, hb);
    }

    private void handleRequest(Message msg) {
        taskPool.submit(() -> {
            try {
                String raw = new String(msg.payload, StandardCharsets.UTF_8);
                String[] parts = raw.split(";", 3);
                String taskId = parts.length > 0 ? parts[0] : "task";
                String taskType = parts.length > 1 ? parts[1] : "MATRIX_MULTIPLY";
                String payload = parts.length > 2 ? parts[2] : "";

                String result;
                if ("MATRIX_MULTIPLY".equalsIgnoreCase(taskType)) {
                    result = multiplyMatrices(payload);
                } else {
                    result = payload;
                }

                Message done = new Message();
                done.messageType = "TASK_COMPLETE";
                done.studentId = studentId;
                done.payload = (taskId + ";" + result).getBytes(StandardCharsets.UTF_8);
                writeMessage(out, done);
            } catch (Exception e) {
                Message err = new Message();
                err.messageType = "TASK_ERROR";
                err.studentId = studentId;
                err.payload = ("unknown;" + e.getMessage()).getBytes(StandardCharsets.UTF_8);
                writeMessage(out, err);
            }
        });
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

    private static String multiplyMatrices(String payload) {
        String[] matrices = payload.split("\\|", 2);
        int[][] a = parseMatrix(matrices.length > 0 ? matrices[0] : "");
        int[][] b = parseMatrix(matrices.length > 1 ? matrices[1] : "");
        int[][] result = multiply(a, b);
        return formatMatrix(result);
    }

    private static int[][] parseMatrix(String text) {
        if (text == null || text.isEmpty()) {
            return new int[0][0];
        }
        String[] rows = text.split("\\\\");
        int[][] matrix = new int[rows.length][];
        for (int i = 0; i < rows.length; i++) {
            String row = rows[i].trim();
            if (row.isEmpty()) {
                matrix[i] = new int[0];
                continue;
            }
            String[] cols = row.split(",");
            matrix[i] = new int[cols.length];
            for (int j = 0; j < cols.length; j++) {
                matrix[i][j] = Integer.parseInt(cols[j].trim());
            }
        }
        return matrix;
    }

    private static int[][] multiply(int[][] a, int[][] b) {
        if (a.length == 0 || b.length == 0) {
            return new int[0][0];
        }
        int rows = a.length;
        int cols = b[0].length;
        int mid = b.length;
        int[][] result = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int k = 0; k < mid; k++) {
                for (int j = 0; j < cols; j++) {
                    result[i][j] += a[i][k] * b[k][j];
                }
            }
        }
        return result;
    }

    private static String formatMatrix(int[][] matrix) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < matrix.length; i++) {
            if (i > 0) {
                sb.append("\\");
            }
            for (int j = 0; j < matrix[i].length; j++) {
                if (j > 0) {
                    sb.append(",");
                }
                sb.append(matrix[i][j]);
            }
        }
        return sb.toString();
    }

    private static String getEnv(String key, String def) {
        String val = System.getenv(key);
        return val == null || val.isEmpty() ? def : val;
    }

    public static void main(String[] args) throws Exception {
        Worker worker = new Worker();
        String host = getEnv("MASTER_HOST", "localhost");
        int port = Integer.parseInt(getEnv("MASTER_PORT", "9999"));
        worker.joinCluster(host, port);
        worker.execute();
    }
}
