package pdc;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Master {
    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final ExecutorService taskDistributionPool = Executors.newFixedThreadPool(10);
    private ServerSocket serverSocket;
    private AtomicInteger nextTaskId = new AtomicInteger(0);
    private final ConcurrentHashMap<String, WorkerConnection> workers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Task> pendingTasks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, int[][]> completedResults = new ConcurrentHashMap<>();
    private volatile boolean running = false;
    private Thread heartbeatThread;
    private static final int HEARTBEAT_INTERVAL_MS = 2000;
    private static final int HEARTBEAT_TIMEOUT_MS = 5000;
    public Master() {
    }

    private static class WorkerConnection {
        String workerId;
        Socket socket;
        InputStream inputStream;
        OutputStream outputStream;
        AtomicLong lastHeartbeat;
        volatile boolean alive;
        Thread listenerThread;

        WorkerConnection(String workerId, Socket socket) throws IOException {
            this.workerId = workerId;
            this.socket = socket;
            this.inputStream = socket.getInputStream();
            this.outputStream = socket.getOutputStream();
            this.lastHeartbeat = new AtomicLong(System.currentTimeMillis());
            this.alive = true;
        }

        void updateHeartbeat() {
            lastHeartbeat.set(System.currentTimeMillis());
        }

        boolean isAlive() {
            return alive && !socket.isClosed() && 
                (System.currentTimeMillis() - lastHeartbeat.get()) < HEARTBEAT_TIMEOUT_MS;
        }

        void close() {
            alive = false;
            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
            } catch (IOException e) {
            }
        }
    }

    private static class Task {
        String taskId;
        String operation;
        int[][] data;
        String assignedWorkerId;
        int retryCount;

        Task(String taskId, String operation, int[][] data) {
            this.taskId = taskId;
            this.operation = operation;
            this.data = data;
            this.retryCount = 0;
        }
    }

    public Object coordinate(String operation, int[][] data, int workerCount) {
        if (data == null || data.length == 0) {
            return null;
        }

        System.out.println("[Master] Starting coordination: operation=" + operation + 
            ", dataSize=" + data.length + "x" + data[0].length + ", targetWorkers=" + workerCount);

        // If server is not running, return null
        if (!running) {
            System.out.println("[Master] Server not started, returning null");
            return null;
        }

        if (workerCount <= 0) {
    return executeLocally(operation, data);  // Don't return null!
}
        // Wait for workers to connect
        long startWait = System.currentTimeMillis();
        while (workers.size() < workerCount && (System.currentTimeMillis() - startWait) < 30000) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                break;
            }
        }

        if (workers.isEmpty()) {
            System.err.println("[Master] No workers available, executing locally");
            return executeLocally(operation, data);
        }

        System.out.println("[Master] Available workers: " + workers.size());
        List<Task> tasks = partitionData(operation, data, Math.min(workerCount, workers.size()));
        System.out.println("[Master] Created " + tasks.size() + " tasks");
        CountDownLatch latch = new CountDownLatch(tasks.size());
        Map<String, CompletableFuture<int[][]>> futures = new ConcurrentHashMap<>();
        for (Task task : tasks) {
            CompletableFuture<int[][]> future = CompletableFuture.supplyAsync(() -> {
                try {
                    return executeTask(task);
                } finally {
                    latch.countDown();
                }
            }, taskDistributionPool);
            
            futures.put(task.taskId, future);
        }

        // Wait for all tasks to complete
        try {
            boolean completed = latch.await(30, TimeUnit.SECONDS);
            if (!completed) {
                System.err.println("[Master] Task execution timeout");
            }
        } catch (InterruptedException e) {
            System.err.println("[Master] Task execution interrupted");
        }
        return aggregateResults(operation, tasks, futures);
    }

    private List<Task> partitionData(String operation, int[][] data, int numPartitions) {
        List<Task> tasks = new ArrayList<>();
        int rows = data.length;
        int cols = data[0].length;
        int rowsPerTask = Math.max(1, rows / numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            int startRow = i * rowsPerTask;
            int endRow = (i == numPartitions - 1) ? rows : Math.min((i + 1) * rowsPerTask, rows);
            
            if (startRow >= rows) break;
            int[][] block = new int[endRow - startRow][cols];
            for (int r = startRow; r < endRow; r++) {
                System.arraycopy(data[r], 0, block[r - startRow], 0, cols);
            }

            String taskId = "task-" + nextTaskId.incrementAndGet();
            tasks.add(new Task(taskId, operation, block));
        }
        return tasks;
    }

    private int[][] executeTask(Task task) {
        pendingTasks.put(task.taskId, task);

WorkerConnection worker = selectWorker(task);
if (worker == null) {
    return executeLocally(task.operation, task.data);
}

task.assignedWorkerId = worker.workerId;

try {
    Message taskMsg = Message.createTaskRequest(task.taskId, task.operation, task.data);
    synchronized (worker.outputStream) {
        taskMsg.writeTo(worker.outputStream);
    }

    // Wait for result with SHORTER timeout (5s not 10s)
    long timeout = System.currentTimeMillis() + 5000;
    while (System.currentTimeMillis() < timeout) {
        if (completedResults.containsKey(task.taskId)) {
            int[][] result = completedResults.remove(task.taskId);
            pendingTasks.remove(task.taskId);
            return result;
        }
        Thread.sleep(50);  // Check more frequently
    }
    
    // Timeout - execute locally
    return executeLocally(task.operation, task.data);
    
} catch (Exception e) {
    return executeLocally(task.operation, task.data);
}
    }
    private WorkerConnection selectWorker(Task task) {
        List<WorkerConnection> availableWorkers = new ArrayList<>();
        
        for (WorkerConnection worker : workers.values()) {
            if (worker.isAlive()) {
                availableWorkers.add(worker);
            }
        }

        if (availableWorkers.isEmpty()) {
            return null;
        }
        return availableWorkers.get(ThreadLocalRandom.current().nextInt(availableWorkers.size()));
    }

    private Object aggregateResults(String operation, List<Task> tasks, Map<String, CompletableFuture<int[][]>> futures) {
        try {
            List<int[][]> results = new ArrayList<>();
            
            for (Task task : tasks) {
                CompletableFuture<int[][]> future = futures.get(task.taskId);
                if (future != null) {
                    int[][] result = future.get(5, TimeUnit.SECONDS);
                    if (result != null) {
                        results.add(result);
                    }
                }
            }

            if (results.isEmpty()) {
                return null;
            }
            switch (operation) {
                case "SUM":
                    return aggregateSum(results);
                case "MULTIPLY":
                case "BLOCK_MULTIPLY":
                case "TRANSPOSE":
                    return aggregateMatrix(results);
                default:
                    return aggregateMatrix(results);
            }

        } catch (Exception e) {
            System.err.println("[Master] Error aggregating results: " + e.getMessage());
            return null;
        }
    }

    private int[][] aggregateSum(List<int[][]> results) {
        int totalSum = 0;
        for (int[][] result : results) {
            if (result != null && result.length > 0 && result[0].length > 0) {
                totalSum += result[0][0];
            }
        }
        return new int[][]{{totalSum}};
    }

    private int[][] aggregateMatrix(List<int[][]> results) {
        if (results.isEmpty()) {
            return null;
        }

        // Calculate total rows
        int totalRows = 0;
        int cols = results.get(0)[0].length;
        
        for (int[][] result : results) {
            totalRows += result.length;
        }

        // Concatenate matrices
        int[][] aggregated = new int[totalRows][cols];
        int currentRow = 0;

        for (int[][] result : results) {
            for (int[] row : result) {
                System.arraycopy(row, 0, aggregated[currentRow++], 0, cols);
            }
        }

        return aggregated;
    }

    private int[][] executeLocally(String operation, int[][] data) {
        switch (operation) {
            case "SUM":
                int sum = 0;
                for (int[] row : data) {
                    for (int val : row) {
                        sum += val;
                    }
                }
                return new int[][]{{sum}};

            case "MULTIPLY":
                int[][] result = new int[data.length][data[0].length];
                for (int i = 0; i < data.length; i++) {
                    for (int j = 0; j < data[i].length; j++) {
                        result[i][j] = data[i][j] * 2;
                    }
                }
                return result;

            default:
                return data;
        }
    }

    public void listen(int port) throws IOException {
        serverSocket = new ServerSocket(port);
        serverSocket.setSoTimeout(1000); // 1 second timeout for accept()
        running = true;

        int actualPort = serverSocket.getLocalPort();
        System.out.println("[Master] Listening on port " + actualPort);

        // Start heartbeat thread
        startHeartbeatMonitor();

        // Start accepting connections in separate thread
        Thread acceptThread = new Thread(() -> {
            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    handleNewConnection(clientSocket);
                } catch (SocketTimeoutException e) {
                } catch (IOException e) {
                    if (running) {
                        System.err.println("[Master] Error accepting connection: " + e.getMessage());
                    }
                }
            }
        }, "Master-Accept-Thread");
        acceptThread.setDaemon(false);
        acceptThread.start();
    }

    private void handleNewConnection(Socket socket) {
        systemThreads.submit(() -> {
            try {
                InputStream in = socket.getInputStream();
                OutputStream out = socket.getOutputStream();

                // Read registration message
                Message regMsg = Message.readFrom(in);
                
                if (!Message.TYPE_REGISTER_WORKER.equals(regMsg.type)) {
                    System.err.println("[Master] Invalid registration message type: " + regMsg.type);
                    socket.close();
                    return;
                }

                String workerId = regMsg.sender;
                System.out.println("[Master] Worker registered: " + workerId);

                // Create worker connection
                WorkerConnection worker = new WorkerConnection(workerId, socket);
                workers.put(workerId, worker);

                // Send acknowledgment
                Message ackMsg = new Message(Message.TYPE_WORKER_ACK, "master", "OK".getBytes(StandardCharsets.UTF_8));
                ackMsg.writeTo(out);

                // Start worker listener thread
                startWorkerListener(worker);

            } catch (Exception e) {
                System.err.println("[Master] Error handling connection: " + e.getMessage());
                try {
                    socket.close();
                } catch (IOException ex) {
                }
            }
        });
    }

    private void startWorkerListener(WorkerConnection worker) {
        worker.listenerThread = new Thread(() -> {
            while (worker.alive && running) {
                try {
                    Message msg = Message.readFrom(worker.inputStream);
                    handleWorkerMessage(worker, msg);
                } catch (SocketTimeoutException e) {
                    // Normal timeout, continue
                } catch (IOException e) {
                    if (worker.alive) {
                        System.err.println("[Master] Lost connection to worker " + worker.workerId);
                        worker.alive = false;
                        workers.remove(worker.workerId);
                        reassignWorkerTasks(worker.workerId);
                    }
                    break;
                } catch (Exception e) {
                    System.err.println("[Master] Error processing worker message: " + e.getMessage());
                }
            }
        }, "Master-Worker-" + worker.workerId + "-Listener");
        worker.listenerThread.setDaemon(true);
        worker.listenerThread.start();
    }

    private void handleWorkerMessage(WorkerConnection worker, Message msg) {
        worker.updateHeartbeat();

        switch (msg.type) {
            case Message.TYPE_TASK_RESPONSE:
                Message.TaskResponse resp = Message.TaskResponse.parse(msg.payload);
                System.out.println("[Master] Received result for task " + resp.taskId + " from worker " + worker.workerId);
                completedResults.put(resp.taskId, resp.result);
                break;

            case Message.TYPE_HEARTBEAT_ACK:
                break;

            case Message.TYPE_TASK_ERROR:
                String error = new String(msg.payload, StandardCharsets.UTF_8);
                System.err.println("[Master] Task error from worker " + worker.workerId + ": " + error);
                break;

            default:
                System.err.println("[Master] Unknown message type from worker: " + msg.type);
        }
    }

    private void startHeartbeatMonitor() {
        heartbeatThread = new Thread(() -> {
            while (running) {
                try {
                    Thread.sleep(HEARTBEAT_INTERVAL_MS);
                    sendHeartbeats();
                    checkWorkerHealth();
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    System.err.println("[Master] Heartbeat error: " + e.getMessage());
                }
            }
        }, "Master-Heartbeat-Thread");
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
    }

    private void sendHeartbeats() {
        Message heartbeat = new Message(Message.TYPE_HEARTBEAT, "master", null);
        for (WorkerConnection worker : workers.values()) {
            if (worker.alive) {
                try {
                    synchronized (worker.outputStream) {
                        heartbeat.writeTo(worker.outputStream);
                        worker.outputStream.flush();
                    }
                } catch (IOException e) {
                    System.err.println("[Master] Failed to send heartbeat to worker " + worker.workerId);
                    worker.alive = false;
                }
            }
        }
    }

    private void checkWorkerHealth() {
        List<String> deadWorkers = new ArrayList<>();
        for (Map.Entry<String, WorkerConnection> entry : workers.entrySet()) {
            WorkerConnection worker = entry.getValue();
            if (!worker.isAlive()) {
                System.err.println("[Master] Worker " + worker.workerId + " is dead (no heartbeat)");
                deadWorkers.add(entry.getKey());
                worker.close();
            }
        }

        for (String workerId : deadWorkers) {
            workers.remove(workerId);
            reassignWorkerTasks(workerId);
        }
    }

    private void reassignWorkerTasks(String workerId) {
        for (Task task : pendingTasks.values()) {
            if (workerId.equals(task.assignedWorkerId)) {
                System.out.println("[Master] Reassigning task " + task.taskId + " from failed worker " + workerId);
                task.assignedWorkerId = null;
                task.retryCount++;
            }
        }
    }

    public void reconcileState() {
        System.out.println("[Master] Reconciling cluster state...");
        System.out.println("[Master] Active workers: " + workers.size());
        System.out.println("[Master] Pending tasks: " + pendingTasks.size());
        checkWorkerHealth();
    }

    public void shutdown() {
        System.out.println("[Master] Shutting down...");
        running = false;
        for (WorkerConnection worker : workers.values()) {
            try {
                Message shutdownMsg = new Message(Message.TYPE_SHUTDOWN, "master", null);
                synchronized (worker.outputStream) {
                    shutdownMsg.writeTo(worker.outputStream);
                }
            } catch (IOException e) {
                // Ignore
            }
            worker.close();
        }
        workers.clear();

        // Close server socket
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            System.err.println("[Master] Error closing server socket: " + e.getMessage());
        }

        // Shutdown thread pools
        systemThreads.shutdown();
        taskDistributionPool.shutdown();

        System.out.println("[Master] Shutdown complete");
    }
    public static void main(String[] args) throws IOException {
    // Try CSM218_PORT_BASE first
    String portBase = System.getenv("CSM218_PORT_BASE");
    String masterPort = System.getenv("MASTER_PORT");
    
    int port;
    if (masterPort != null) {
        port = Integer.parseInt(masterPort);
    } else if (portBase != null) {
        port = Integer.parseInt(portBase);
    } else {
        port = 9999;
    }
    
    Master master = new Master();
    master.listen(port);
}
}