package com.quant;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.quant.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class WorkerApp {

    private static final String MASTER_HOST = "localhost";
    private static final int MASTER_PORT = 50051;
    private static final int BASE_DAEMON_PORT = 9100;

    private static final List<Process> daemonProcesses = new CopyOnWriteArrayList<>();
    private static final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private static ScheduledExecutorService heartbeatScheduler;
    private static ExecutorService threadPool;

    public static void main(String[] args) throws Exception {
        String workerId = (args.length > 0) ? args[0] : "Worker-" + UUID.randomUUID().toString().substring(0, 6).toUpperCase();
        System.out.println("DISTRIBUTED TASK EXECUTION ENGINE - WORKER: " + workerId);

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(MASTER_HOST, MASTER_PORT)
                .usePlaintext()
                .maxInboundMessageSize(16 * 1024 * 1024)
                .build();
        TaskServiceGrpc.TaskServiceBlockingStub stub = TaskServiceGrpc.newBlockingStub(channel);

        int cpuCores = Runtime.getRuntime().availableProcessors();
        System.out.printf("Detected %d CPU cores. Creating fixed thread pool.%n", cpuCores);

        File daemonScript = resolveDaemonScript();
        System.out.println("Daemon script: " + daemonScript.getAbsolutePath());

        System.out.println("Registering with Master at " + MASTER_HOST + ":" + MASTER_PORT);
        RegistrationAck ack = stub.registerWorker(
            WorkerRegistration.newBuilder()
                .setWorkerId(workerId)
                .setThreadCount(cpuCores)
                .build());
        if (!ack.getAccepted()) {
            System.err.println("Registration rejected: " + ack.getMessage());
            return;
        }
        System.out.println("Registered. " + ack.getMessage());

        int portOffset = new Random().nextInt(500) * cpuCores;
        int actualBasePort = BASE_DAEMON_PORT + portOffset;

        threadPool = Executors.newFixedThreadPool(cpuCores, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });

        heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "heartbeat-scheduler");
            t.setDaemon(true);
            return t;
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shuttingDown.set(true);
            heartbeatScheduler.shutdownNow();
            threadPool.shutdownNow();
            for (Process p : daemonProcesses) { if (p.isAlive()) p.destroyForcibly(); }
            channel.shutdown();
        }));

        for (int i = 0; i < cpuCores; i++) {
            final int daemonPort = actualBasePort + i;
            final String threadId = String.format("%s-T%02d", workerId, i);
            threadPool.submit(() -> runThreadLoop(threadId, workerId, daemonPort, daemonScript, stub));
        }

        threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }

    private static void runThreadLoop(String threadId, String workerId, int port,
                                       File daemonScript, TaskServiceGrpc.TaskServiceBlockingStub stub) {
        System.out.printf("[%s] Starting on daemon port %d%n", threadId, port);

        Process daemon = startDaemonProcess(daemonScript, port, threadId);
        if (daemon == null) {
            System.err.printf("[%s] Failed to start Python daemon. Exiting thread.%n", threadId);
            return;
        }
        daemonProcesses.add(daemon);

        Socket socket = connectToDaemon(port, threadId, 20, 200);
        if (socket == null) {
            System.err.printf("[%s] Could not connect to daemon on port %d.%n", threadId, port);
            daemon.destroyForcibly();
            return;
        }

        System.out.printf("[%s] Connected to daemon on port %d%n", threadId, port);

        ScheduledFuture<?> heartbeatTask = heartbeatScheduler.scheduleAtFixedRate(() -> {
            if (shuttingDown.get()) return;
            try {
                stub.sendHeartbeat(WorkerHeartbeat.newBuilder()
                    .setWorkerId(workerId)
                    .setThreadId(threadId)
                    .setStatus("IDLE")
                    .build());
            } catch (Exception e) {}
        }, 0, 2, TimeUnit.SECONDS);

        ObjectMapper mapper = new ObjectMapper();

        try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            while (!shuttingDown.get() && daemon.isAlive()) {
                JobAssignment job;
                try {
                    job = stub.pollJob(PollRequest.newBuilder()
                        .setWorkerId(workerId)
                        .setThreadId(threadId)
                        .build());
                } catch (Exception e) {
                    System.err.printf("[%s] Poll error: %s - retrying in 3s%n", threadId, e.getMessage());
                    Thread.sleep(3_000);
                    continue;
                }

                if (job == null || job.getJobId().isEmpty()) {
                    Thread.sleep(300);
                    continue;
                }

                System.out.printf("[%s] -> %s | %s | complexity: %,d%n",
                    threadId, job.getJobId(), job.getJobType(), job.getComplexity());

                long startMs = System.currentTimeMillis();

                Map<String, Object> ipcRequest = new LinkedHashMap<>();
                ipcRequest.put("job_id", job.getJobId());
                ipcRequest.put("job_type", job.getJobType());
                ipcRequest.put("complexity", job.getComplexity());
                ipcRequest.put("payload_json", job.getPayloadJson());

                writer.println(mapper.writeValueAsString(ipcRequest));

                String rawResponse = reader.readLine();
                long durationMs = System.currentTimeMillis() - startMs;

                if (rawResponse == null) {
                    System.err.printf("[%s] Daemon connection closed.%n", threadId);
                    break;
                }

                JobResult result = parseAndBuildResult(rawResponse, mapper, job, workerId, threadId, durationMs);

                if (result.getSuccess()) {
                    System.out.printf("[%s] Done %s in %dms | ops: %,d%n",
                        threadId, job.getJobId(), durationMs, result.getOperationsCount());
                } else {
                    System.err.printf("[%s] Failed %s: %s%n", threadId, job.getJobId(), result.getErrorMessage());
                }

                try {
                    stub.submitResult(result);
                } catch (Exception e) {
                    System.err.printf("[%s] Submit failed: %s%n", threadId, e.getMessage());
                }
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.err.printf("[%s] Exception: %s%n", threadId, e.getMessage());
        } finally {
            heartbeatTask.cancel(false);
            try { socket.close(); } catch (Exception ignored) {}
            if (daemon.isAlive()) daemon.destroyForcibly();
        }
    }

    private static Process startDaemonProcess(File script, int port, String threadId) {
        try {
            ProcessBuilder pb = new ProcessBuilder("python", script.getAbsolutePath(), "--port", String.valueOf(port));
            pb.directory(script.getParentFile());
            pb.redirectError(ProcessBuilder.Redirect.INHERIT);
            Process p = pb.start();
            Thread stdoutDrain = new Thread(() -> {
                try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                    String line;
                    while ((line = r.readLine()) != null) {
                        System.out.println("[DAEMON-" + port + "] " + line);
                    }
                } catch (Exception ignored) {}
            });
            stdoutDrain.setDaemon(true);
            stdoutDrain.start();
            return p;
        } catch (Exception e) {
            System.err.printf("[%s] Could not start daemon: %s%n", threadId, e.getMessage());
            return null;
        }
    }

    private static Socket connectToDaemon(int port, String threadId, int maxRetries, int delayMs) {
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {
                Socket s = new Socket("127.0.0.1", port);
                s.setTcpNoDelay(true);
                return s;
            } catch (Exception e) {
                try { Thread.sleep(delayMs); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
        }
        return null;
    }

    private static File resolveDaemonScript() {
        for (String candidate : new String[]{"compute_daemon.py", "../compute_daemon.py"}) {
            File f = new File(candidate);
            if (f.exists()) return f;
        }
        return new File("compute_daemon.py");
    }

    private static JobResult parseAndBuildResult(String rawJson, ObjectMapper mapper,
                                                   JobAssignment job, String workerId,
                                                   String threadId, long durationMs) {
        JobResult.Builder builder = JobResult.newBuilder()
            .setJobId(job.getJobId())
            .setWorkerId(workerId)
            .setThreadId(threadId)
            .setExecutionTimeMs(durationMs);
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> resp = mapper.readValue(rawJson, Map.class);
            boolean success = Boolean.TRUE.equals(resp.get("success"));
            builder.setSuccess(success);
            if (success) {
                long ops = resp.get("operations_count") != null ? ((Number) resp.get("operations_count")).longValue() : 0L;
                String summary = resp.getOrDefault("output_summary", "").toString();
                builder.setOperationsCount(ops).setOutputSummary(summary);
            } else {
                String err = resp.getOrDefault("error_message", "Unknown error").toString();
                builder.setErrorMessage(err);
            }
        } catch (Exception e) {
            builder.setSuccess(false).setErrorMessage("Parse error: " + e.getMessage());
        }
        return builder.build();
    }
}
