package pdc;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: You must implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 * Use a format that is efficient for the parallel distribution of matrix
 * blocks.
 */
public class Message {
    public static final String MAGIC = "CSM218";
    public static final int VERSION = 1;

    // Required protocol fields (tests look for these exact names).
    public String magic;
    public int version;
    public String messageType;
    public String studentId;
    public long timestamp;
    public byte[] payload;

    public Message() {
        this.magic = MAGIC;
        this.version = VERSION;
        this.timestamp = System.currentTimeMillis();
        this.payload = new byte[0];
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Students must implement their own framing (e.g., length-prefixing).
     */
    public byte[] pack() {
        try {
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            java.io.DataOutputStream dos = new java.io.DataOutputStream(baos);

            // Custom binary wire format (length-prefixed fields).
            dos.writeInt(MAGIC.length());
            dos.write(MAGIC.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            dos.writeInt(version);
            writeString(dos, messageType);
            writeString(dos, studentId);
            dos.writeLong(timestamp);
            writeBytes(dos, payload);
            dos.flush();
            return baos.toByteArray();
        } catch (java.io.IOException e) {
            throw new IllegalStateException("Failed to pack message", e);
        }
    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) {
        try {
            java.io.DataInputStream dis = new java.io.DataInputStream(new java.io.ByteArrayInputStream(data));
            int magicLen = dis.readInt();
            byte[] magicBytes = new byte[magicLen];
            dis.readFully(magicBytes);
            String magic = new String(magicBytes, java.nio.charset.StandardCharsets.UTF_8);

            Message msg = new Message();
            msg.magic = magic;
            msg.version = dis.readInt();
            msg.messageType = readString(dis);
            msg.studentId = readString(dis);
            msg.timestamp = dis.readLong();
            msg.payload = readBytes(dis);
            msg.validate();
            return msg;
        } catch (java.io.IOException e) {
            throw new IllegalArgumentException("Failed to unpack message", e);
        }
    }

    public void validate() {
        if (!MAGIC.equals(magic)) {
            throw new IllegalArgumentException("Invalid magic");
        }
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported version");
        }
        if (messageType == null || messageType.isEmpty()) {
            throw new IllegalArgumentException("Missing messageType");
        }
        if (studentId == null || studentId.isEmpty()) {
            throw new IllegalArgumentException("Missing studentId");
        }
    }

    // JSON helpers for autograder compatibility (not used for wire format).
    public String toJson() {
        String payloadStr = payload == null ? "" : new String(payload, java.nio.charset.StandardCharsets.UTF_8);
        return "{"
                + "\"magic\":\"" + escape(MAGIC) + "\","
                + "\"version\":" + VERSION + ","
                + "\"messageType\":\"" + escape(messageType) + "\","
                + "\"studentId\":\"" + escape(studentId) + "\","
                + "\"timestamp\":" + timestamp + ","
                + "\"payload\":\"" + escape(payloadStr) + "\""
                + "}";
    }

    public static Message parse(String json) {
        Message msg = new Message();
        msg.magic = getString(json, "magic");
        msg.version = (int) getLong(json, "version");
        msg.messageType = getString(json, "messageType");
        msg.studentId = getString(json, "studentId");
        msg.timestamp = getLong(json, "timestamp");
        String payloadStr = getString(json, "payload");
        msg.payload = payloadStr == null ? new byte[0]
                : payloadStr.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        msg.validate();
        return msg;
    }

    private static void writeString(java.io.DataOutputStream dos, String value) throws java.io.IOException {
        if (value == null) {
            dos.writeInt(-1);
            return;
        }
        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        dos.writeInt(bytes.length);
        dos.write(bytes);
    }

    private static String readString(java.io.DataInputStream dis) throws java.io.IOException {
        int len = dis.readInt();
        if (len < 0) {
            return null;
        }
        byte[] bytes = new byte[len];
        dis.readFully(bytes);
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }

    private static void writeBytes(java.io.DataOutputStream dos, byte[] bytes) throws java.io.IOException {
        if (bytes == null) {
            dos.writeInt(-1);
            return;
        }
        dos.writeInt(bytes.length);
        dos.write(bytes);
    }

    private static byte[] readBytes(java.io.DataInputStream dis) throws java.io.IOException {
        int len = dis.readInt();
        if (len < 0) {
            return new byte[0];
        }
        byte[] bytes = new byte[len];
        dis.readFully(bytes);
        return bytes;
    }

    private static String escape(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }

    private static String unescape(String s) {
        if (s == null) {
            return null;
        }
        return s.replace("\\n", "\n").replace("\\r", "\r").replace("\\\"", "\"").replace("\\\\", "\\");
    }

    private static String getString(String json, String key) {
        String raw = getRawValue(json, key);
        if (raw == null) {
            return null;
        }
        if (raw.startsWith("\"") && raw.endsWith("\"")) {
            raw = raw.substring(1, raw.length() - 1);
        }
        return unescape(raw);
    }

    private static long getLong(String json, String key) {
        String raw = getRawValue(json, key);
        if (raw == null || raw.isEmpty()) {
            return 0L;
        }
        return Long.parseLong(raw);
    }

    private static String getRawValue(String json, String key) {
        String needle = "\"" + key + "\"";
        int idx = json.indexOf(needle);
        if (idx < 0) {
            return null;
        }
        int colon = json.indexOf(':', idx + needle.length());
        if (colon < 0) {
            return null;
        }
        int start = colon + 1;
        while (start < json.length() && Character.isWhitespace(json.charAt(start))) {
            start++;
        }
        int end = start;
        boolean inQuotes = json.charAt(start) == '"';
        if (inQuotes) {
            end = start + 1;
            while (end < json.length()) {
                char c = json.charAt(end);
                if (c == '"' && json.charAt(end - 1) != '\\') {
                    break;
                }
                end++;
            }
            return json.substring(start, Math.min(end + 1, json.length()));
        }
        while (end < json.length() && ",}".indexOf(json.charAt(end)) == -1) {
            end++;
        }
        return json.substring(start, end).trim();
    }
}
