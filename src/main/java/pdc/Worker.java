package pdc;
import java.io.*;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Worker {
    private String workerId;
    @SuppressWarnings("unused") // Stored for potential reconnection logic
    private String masterHost;
    @SuppressWarnings("unused") // Stored for potential reconnection logic
    private int masterPort;
    private Socket socket;
    private InputStream inputStream;
    private OutputStream outputStream;
    private ExecutorService executorService;
    private AtomicBoolean running;
    private Thread listenerThread;
    private Thread heartbeatThread;

    public Worker() {
        this.workerId = System.getenv().getOrDefault("WORKER_ID", "worker-" + System.currentTimeMillis());
        this.executorService = Executors.newFixedThreadPool(4);
        this.running = new AtomicBoolean(false);
    }

    public Worker(String workerId) {
        this.workerId = workerId;
        this.executorService = Executors.newFixedThreadPool(4);
        this.running = new AtomicBoolean(false);
    }

    public void joinCluster(String masterHost, int port) {
        this.masterHost = masterHost;
        this.masterPort = port;
        try {
            // Connect to master
            socket = new Socket(masterHost, port);
            socket.setSoTimeout(30000); // 30 second timeout
            inputStream = socket.getInputStream();
            outputStream = socket.getOutputStream();
            System.out.println("[Worker " + workerId + "] Connected to master at " + masterHost + ":" + port);
            Message registerMsg = new Message(Message.TYPE_REGISTER_WORKER, workerId, 
                workerId.getBytes(StandardCharsets.UTF_8));
            registerMsg.writeTo(outputStream);
            Message ackMsg = Message.readFrom(inputStream);
            if (Message.TYPE_WORKER_ACK.equals(ackMsg.type)) {
                System.out.println("[Worker " + workerId + "] Registration acknowledged");
                running.set(true);
                startListenerThread();
                startHeartbeatThread();
            } else {
                System.err.println("[Worker " + workerId + "] Registration failed: unexpected response " + ackMsg.type);
                close();
            }
        } catch (IOException e) {
            System.err.println("[Worker " + workerId + "] Failed to join cluster: " + e.getMessage());
            close();
        }
    }

    private void startListenerThread() {
        listenerThread = new Thread(() -> {
            while (running.get()) {
                try {
                    Message msg = Message.readFrom(inputStream);
                    handleMessage(msg);
                } catch (SocketTimeoutException e) {
                    // Timeout is normal, just continue
                    continue;
                } catch (IOException e) {
                    if (running.get()) {
                        System.err.println("[Worker " + workerId + "] Connection lost: " + e.getMessage());
                        running.set(false);
                    }
                    break;
                } catch (Exception e) {
                    System.err.println("[Worker " + workerId + "] Error processing message: " + e.getMessage());
                    e.printStackTrace();
                }
            }
            System.out.println("[Worker " + workerId + "] Listener thread stopped");
        }, "Worker-" + workerId + "-Listener");
        listenerThread.setDaemon(true);
        listenerThread.start();
    }

    private void startHeartbeatThread() {
        heartbeatThread = new Thread(() -> {
        }, "Worker-" + workerId + "-Heartbeat");
        heartbeatThread.setDaemon(true);
    }

    private void handleMessage(Message msg) {
        switch (msg.type) {
            case Message.TYPE_TASK_REQUEST:
                executorService.submit(() -> processTask(msg));
                break;
            case Message.TYPE_HEARTBEAT:
    try {
        Message ackMsg = new Message(Message.TYPE_HEARTBEAT_ACK, workerId, null);
        ackMsg.studentId = workerId;  // ADD THIS LINE
        synchronized (outputStream) {
            ackMsg.writeTo(outputStream);
        }
    } catch (IOException e) {
                    System.err.println("[Worker " + workerId + "] Failed to send heartbeat ack: " + e.getMessage());
                }
                break;
            case Message.TYPE_SHUTDOWN:
                System.out.println("[Worker " + workerId + "] Received shutdown signal");
                running.set(false);
                close();
                break;
            default:
                System.err.println("[Worker " + workerId + "] Unknown message type: " + msg.type);
        }
    }

    private void processTask(Message taskMsg) {
        try {
            Message.TaskRequest taskReq = Message.TaskRequest.parse(taskMsg.payload);
            
            System.out.println("[Worker " + workerId + "] Processing task " + taskReq.taskId + 
                " operation: " + taskReq.operation);
            int[][] result = performComputation(taskReq.operation, taskReq.matrix);
            Message responseMsg = Message.createTaskResponse(taskReq.taskId, result);
            responseMsg.sender = workerId;
            synchronized (outputStream) {
                responseMsg.writeTo(outputStream);
                outputStream.flush();
            }
            System.out.println("[Worker " + workerId + "] Completed task " + taskReq.taskId);
        } catch (Exception e) {
            System.err.println("[Worker " + workerId + "] Task processing failed: " + e.getMessage());
            e.printStackTrace();
            try {
                Message errorMsg = new Message(Message.TYPE_TASK_ERROR, workerId, 
                    e.getMessage().getBytes(StandardCharsets.UTF_8));
                synchronized (outputStream) {
                    errorMsg.writeTo(outputStream);
                }
            } catch (IOException ioEx) {
                System.err.println("[Worker " + workerId + "] Failed to send error message: " + ioEx.getMessage());
            }
        }
    }

    private int[][] performComputation(String operation, int[][] matrix) {
        switch (operation) {
            case "SUM":
                return computeSum(matrix);
            case "MULTIPLY":
                return computeMultiply(matrix);
            case "BLOCK_MULTIPLY":
                return computeBlockMultiply(matrix);
            case "TRANSPOSE":
                return computeTranspose(matrix);
            default:
                System.err.println("[Worker " + workerId + "] Unknown operation: " + operation);
                return matrix;
        }
    }

    private int[][] computeSum(int[][] matrix) {
        int sum = 0;
        for (int[] row : matrix) {
            for (int val : row) {
                sum += val;
            }
        }
        return new int[][]{{sum}};
    }

    private int[][] computeMultiply(int[][] matrix) {
        int[][] result = new int[matrix.length][matrix[0].length];
        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix[i].length; j++) {
                result[i][j] = matrix[i][j] * 2;
            }
        }
        return result;
    }

    private int[][] computeBlockMultiply(int[][] matrix) {
        // For now, just square each element (placeholder for real matrix multiplication)
        int[][] result = new int[matrix.length][matrix[0].length];
        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix[i].length; j++) {
                result[i][j] = matrix[i][j] * matrix[i][j];
            }
        }
        return result;
    }

    private int[][] computeTranspose(int[][] matrix) {
        int rows = matrix.length;
        int cols = matrix[0].length;
        int[][] result = new int[cols][rows];
        
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                result[j][i] = matrix[i][j];
            }
        }
        return result;
    }

    public void execute() {
        while (running.get()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    public void close() {
        running.set(false);
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            System.err.println("[Worker " + workerId + "] Error closing socket: " + e.getMessage());
        }
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
        System.out.println("[Worker " + workerId + "] Shutdown complete");
    }

    public static void main(String[] args) {
    String workerId = System.getenv("WORKER_ID");
    if (workerId == null) {
        workerId = "worker-" + System.currentTimeMillis();
    }
    
    String masterHost = System.getenv("MASTER_HOST");
    if (masterHost == null) {
        masterHost = "localhost";
    }
    
    String masterPort = System.getenv("MASTER_PORT");
    String portBase = System.getenv("CSM218_PORT_BASE");
    
    int port;
    if (masterPort != null) {
        port = Integer.parseInt(masterPort);
    } else if (portBase != null) {
        port = Integer.parseInt(portBase);
    } else {
        port = 9999;
    }
}
}