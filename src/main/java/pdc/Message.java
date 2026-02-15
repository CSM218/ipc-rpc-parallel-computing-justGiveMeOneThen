package pdc;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class Message {
    public String magic;
    public int version;
    public String type;
    public String messageType;
    public String sender;
    public String studentId;
    public long timestamp;
    public byte[] payload;

    public static final String TYPE_CONNECT = "CONNECT";
    public static final String TYPE_REGISTER_WORKER = "REGISTER_WORKER";
    public static final String TYPE_WORKER_ACK = "WORKER_ACK";
    public static final String TYPE_TASK_REQUEST = "TASK_REQUEST";
    public static final String TYPE_TASK_RESPONSE = "TASK_RESPONSE";
    public static final String TYPE_HEARTBEAT = "HEARTBEAT";
    public static final String TYPE_HEARTBEAT_ACK = "HEARTBEAT_ACK";
    public static final String TYPE_TASK_COMPLETE = "TASK_COMPLETE";
    public static final String TYPE_TASK_ERROR = "TASK_ERROR";
    public static final String TYPE_SHUTDOWN = "SHUTDOWN";

    public Message() {
    this.magic = "CSM218";
    this.version = 1;
    this.timestamp = System.currentTimeMillis();
    
    // CRITICAL: Read from environment
    String envStudentId = System.getenv("STUDENT_ID");
    if (envStudentId != null && !envStudentId.isEmpty()) {
        this.studentId = envStudentId;
    } else {
        this.studentId = "default-student";
    }
    }

    public Message(String type, String sender, byte[] payload) {
        this();
        this.type = type;
        this.messageType = type;
        this.sender = sender;
        this.studentId = sender != null ? sender : this.studentId;
        this.payload = payload;
    }

    public byte[] pack() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            
            byte[] magicBytes = magic.getBytes(StandardCharsets.UTF_8);
            dos.write(magicBytes, 0, Math.min(6, magicBytes.length));
            for (int i = magicBytes.length; i < 6; i++) {
                dos.writeByte(0);
            }
            
            dos.writeInt(version);
            
            byte[] typeBytes = (type != null ? type : "").getBytes(StandardCharsets.UTF_8);
            dos.writeInt(typeBytes.length);
            dos.write(typeBytes);
            
            byte[] senderBytes = (sender != null ? sender : "").getBytes(StandardCharsets.UTF_8);
            dos.writeInt(senderBytes.length);
            dos.write(senderBytes);
            
            dos.writeLong(timestamp);
            
            if (payload != null) {
                dos.writeInt(payload.length);
                dos.write(payload);
            } else {
                dos.writeInt(0);
            }

            byte[] messageBody = baos.toByteArray();
            ByteArrayOutputStream finalStream = new ByteArrayOutputStream();
            DataOutputStream finalDos = new DataOutputStream(finalStream);
            finalDos.writeInt(messageBody.length);
            finalDos.write(messageBody);
            return finalStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to pack message", e);
        }
    }

    public static Message unpack(byte[] data) {
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
            Message msg = new Message();
            
            byte[] magicBytes = new byte[6];
            dis.readFully(magicBytes);
            int magicLen = 6;
            for (int i = 0; i < 6; i++) {
                if (magicBytes[i] == 0) {
                    magicLen = i;
                    break;
                }
            }
            msg.magic = new String(magicBytes, 0, magicLen, StandardCharsets.UTF_8);
            msg.version = dis.readInt();
            
            int typeLen = dis.readInt();
            if (typeLen > 0) {
                byte[] typeBytes = new byte[typeLen];
                dis.readFully(typeBytes);
                msg.type = new String(typeBytes, StandardCharsets.UTF_8);
                msg.messageType = msg.type;
            }

            int senderLen = dis.readInt();
            if (senderLen > 0) {
                byte[] senderBytes = new byte[senderLen];
                dis.readFully(senderBytes);
                msg.sender = new String(senderBytes, StandardCharsets.UTF_8);
                msg.studentId = msg.sender;
            }
            
            msg.timestamp = dis.readLong();
            
            int payloadLen = dis.readInt();
            if (payloadLen > 0) {
                msg.payload = new byte[payloadLen];
                dis.readFully(msg.payload);
            }
            return msg;
        } catch (IOException e) {
            throw new RuntimeException("Failed to unpack message", e);
        }
    }

    public static Message readFrom(InputStream in) throws IOException {
        DataInputStream dis = new DataInputStream(in);
        int totalLength = dis.readInt();
        if (totalLength <= 0 || totalLength > 100 * 1024 * 1024) {
            throw new IOException("Invalid message length: " + totalLength);
        }
        byte[] messageBody = new byte[totalLength];
        dis.readFully(messageBody);
        return unpack(messageBody);
    }

    public void writeTo(OutputStream out) throws IOException {
        byte[] packed = pack();
        out.write(packed);
        out.flush();
    }

    public void validate() throws IllegalArgumentException {
        if (!"CSM218".equals(magic)) {
            throw new IllegalArgumentException("Invalid magic: " + magic);
        }
        if (version != 1) {
            throw new IllegalArgumentException("Invalid version: " + version);
        }
        if (type == null || type.isEmpty()) {
            throw new IllegalArgumentException("Message type cannot be null or empty");
        }
    }

    public static Message createTaskRequest(String taskId, String operation, int[][] matrixBlock) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            
            byte[] taskIdBytes = taskId.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(taskIdBytes.length);
            dos.write(taskIdBytes);
            
            byte[] opBytes = operation.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(opBytes.length);
            dos.write(opBytes);
            
            dos.writeInt(matrixBlock.length);
            dos.writeInt(matrixBlock[0].length);
            
            for (int[] row : matrixBlock) {
                for (int val : row) {
                    dos.writeInt(val);
                }
            }
            
            return new Message(TYPE_TASK_REQUEST, "master", baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create task request", e);
        }
    }

    public static class TaskRequest {
        public String taskId;
        public String operation;
        public int[][] matrix;
        
        public static TaskRequest parse(byte[] payload) {
            try {
                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(payload));
                TaskRequest req = new TaskRequest();
                
                int taskIdLen = dis.readInt();
                byte[] taskIdBytes = new byte[taskIdLen];
                dis.readFully(taskIdBytes);
                req.taskId = new String(taskIdBytes, StandardCharsets.UTF_8);
                
                int opLen = dis.readInt();
                byte[] opBytes = new byte[opLen];
                dis.readFully(opBytes);
                req.operation = new String(opBytes, StandardCharsets.UTF_8);
                
                int rows = dis.readInt();
                int cols = dis.readInt();
                req.matrix = new int[rows][cols];
                
                for (int i = 0; i < rows; i++) {
                    for (int j = 0; j < cols; j++) {
                        req.matrix[i][j] = dis.readInt();
                    }
                }
                return req;
            } catch (IOException e) {
                throw new RuntimeException("Failed to parse task request", e);
            }
        }
    }

    public static Message createTaskResponse(String taskId, int[][] result) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            
            byte[] taskIdBytes = taskId.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(taskIdBytes.length);
            dos.write(taskIdBytes);
            
            dos.writeInt(result.length);
            dos.writeInt(result[0].length);
            
            for (int[] row : result) {
                for (int val : row) {
                    dos.writeInt(val);
                }
            }
            
            return new Message(TYPE_TASK_RESPONSE, "worker", baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create task response", e);
        }
    }

    public static class TaskResponse {
        public String taskId;
        public int[][] result;
        
        public static TaskResponse parse(byte[] payload) {
            try {
                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(payload));
                TaskResponse resp = new TaskResponse();
                
                int taskIdLen = dis.readInt();
                byte[] taskIdBytes = new byte[taskIdLen];
                dis.readFully(taskIdBytes);
                resp.taskId = new String(taskIdBytes, StandardCharsets.UTF_8);
                
                int rows = dis.readInt();
                int cols = dis.readInt();
                resp.result = new int[rows][cols];
                
                for (int i = 0; i < rows; i++) {
                    for (int j = 0; j < cols; j++) {
                        resp.result[i][j] = dis.readInt();
                    }
                }
                return resp;
            } catch (IOException e) {
                throw new RuntimeException("Failed to parse task response", e);
            }
        }
    }

    @Override
    public String toString() {
        return String.format("Message[magic=%s, version=%d, type=%s, sender=%s, timestamp=%d, payloadSize=%d]",
                magic, version, type, sender, timestamp, (payload != null ? payload.length : 0));
    }
}