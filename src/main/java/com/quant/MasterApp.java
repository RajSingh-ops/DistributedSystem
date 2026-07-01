package com.quant;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.quant.grpc.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class MasterApp {

    private static final int GRPC_PORT = 50051;
    private static final int WS_PORT = 8081;
    private static final long HEARTBEAT_TIMEOUT_MS = 7_000;
    private static final int TOTAL_JOBS = 1_000;

    private static final String[] JOB_TYPES = {
        "HASH_CRACKING", "LOG_PARSING", "SORT_BENCHMARK", "PRIME_SIEVE"
    };

    private static final PriorityBlockingQueue<JobAssignment> jobQueue =
        new PriorityBlockingQueue<>(TOTAL_JOBS + 1,
            Comparator.comparingLong((JobAssignment j) -> j.getComplexity()).reversed());

    private static final ConcurrentHashMap<String, WorkerNode> workers = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, JobResult> completedJobs = new ConcurrentHashMap<>();

    private static final AtomicLong totalOpsCompleted = new AtomicLong(0);
    private static final AtomicLong totalExceptions = new AtomicLong(0);
    private static final AtomicLong totalNetworkDrops = new AtomicLong(0);
    private static final AtomicLong opsInLastSecond = new AtomicLong(0);
    private static final AtomicLong jobsInLastSecond = new AtomicLong(0);
    private static volatile long currentOpsPerSec = 0;
    private static volatile long currentDrainRate = 0;

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static ClusterWebSocketServer wsServer;

    static class WorkerNode {
        String workerId;
        int declaredThreadCount;
        final ConcurrentHashMap<String, ThreadState> threads = new ConcurrentHashMap<>();
    }

    static class ThreadState {
        String threadId;
        String workerId;
        volatile long lastHeartbeatMs = System.currentTimeMillis();
        volatile String status = "IDLE";
        volatile String currentJobId = "";
        volatile JobAssignment currentJob = null;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("DISTRIBUTED TASK EXECUTION ENGINE - MASTER NODE");

        seedJobQueue(TOTAL_JOBS);
        System.out.printf("Seeded priority queue with %,d jobs.%n", TOTAL_JOBS);

        Server grpcServer = ServerBuilder.forPort(GRPC_PORT)
                .addService(new TaskServiceImpl())
                .maxInboundMessageSize(16 * 1024 * 1024)
                .build()
                .start();
        System.out.println("gRPC server on port " + GRPC_PORT);

        wsServer = new ClusterWebSocketServer(new InetSocketAddress(WS_PORT));
        wsServer.start();
        System.out.println("WebSocket server on port " + WS_PORT);

        startFaultToleranceDaemon();
        startMetricsSamplerDaemon();
        System.out.println("Master fully operational.");

        grpcServer.awaitTermination();
    }

    private static void seedJobQueue(int count) {
        Random rng = new Random(42);
        for (int i = 1; i <= count; i++) {
            String jobId = String.format("JOB-%04d", i);
            String jobType = JOB_TYPES[i % JOB_TYPES.length];
            long complexity = 10_000L + (long)(rng.nextDouble() * 1_990_000L);

            Map<String, Object> payload = new HashMap<>();
            payload.put("seed", rng.nextInt(100_000));

            String payloadJson;
            try {
                payloadJson = objectMapper.writeValueAsString(payload);
            } catch (Exception e) {
                payloadJson = "{}";
            }

            jobQueue.offer(JobAssignment.newBuilder()
                .setJobId(jobId)
                .setJobType(jobType)
                .setComplexity(complexity)
                .setPayloadJson(payloadJson)
                .build());
        }
    }

    private static void startFaultToleranceDaemon() {
        Thread daemon = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(1_000);
                    long now = System.currentTimeMillis();
                    boolean changed = false;

                    for (WorkerNode node : workers.values()) {
                        for (ThreadState ts : node.threads.values()) {
                            if ("DISCONNECTED".equals(ts.status)) continue;
                            long silence = now - ts.lastHeartbeatMs;
                            if (silence > HEARTBEAT_TIMEOUT_MS) {
                                System.out.printf("[FAULT] Thread %s silent for %dms -> DISCONNECTED%n", ts.threadId, silence);
                                ts.status = "DISCONNECTED";
                                totalNetworkDrops.incrementAndGet();
                                changed = true;
                                if (ts.currentJob != null) {
                                    System.out.printf("[RESCUE] Re-queuing %s from dead thread %s%n", ts.currentJobId, ts.threadId);
                                    jobQueue.offer(ts.currentJob);
                                    ts.currentJob = null;
                                    ts.currentJobId = "";
                                }
                            }
                        }
                    }
                    if (changed) broadcastTelemetry();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    System.err.println("[FAULT DAEMON] " + e.getMessage());
                }
            }
        }, "fault-tolerance-daemon");
        daemon.setDaemon(true);
        daemon.start();
    }

    private static void startMetricsSamplerDaemon() {
        Thread sampler = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(1_000);
                    currentOpsPerSec = opsInLastSecond.getAndSet(0);
                    currentDrainRate = jobsInLastSecond.getAndSet(0);
                    broadcastTelemetry();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    System.err.println("[METRICS] " + e.getMessage());
                }
            }
        }, "metrics-sampler-daemon");
        sampler.setDaemon(true);
        sampler.start();
    }

    static void broadcastTelemetry() {
        if (wsServer == null) return;
        try {
            String json = objectMapper.writeValueAsString(buildTelemetryPayload());
            wsServer.broadcast(json);
        } catch (Exception e) {
            System.err.println("[WS BROADCAST] " + e.getMessage());
        }
    }

    private static Map<String, Object> buildTelemetryPayload() {
        Map<String, Object> payload = new LinkedHashMap<>();

        List<Map<String, Object>> workerList = new ArrayList<>();
        int totalThreads = 0;
        int busyThreads = 0;
        int idleThreads = 0;
        int deadThreads = 0;

        for (WorkerNode node : workers.values()) {
            Map<String, Object> wm = new LinkedHashMap<>();
            wm.put("workerId", node.workerId);
            wm.put("declaredThreads", node.declaredThreadCount);

            List<Map<String, Object>> threadList = new ArrayList<>();
            for (ThreadState ts : node.threads.values()) {
                Map<String, Object> tm = new LinkedHashMap<>();
                tm.put("threadId", ts.threadId);
                tm.put("status", ts.status);
                tm.put("currentJobId", ts.currentJobId);
                tm.put("heartbeatAgeMs", System.currentTimeMillis() - ts.lastHeartbeatMs);
                threadList.add(tm);
                totalThreads++;
                switch (ts.status) {
                    case "BUSY":         busyThreads++;  break;
                    case "IDLE":         idleThreads++;  break;
                    case "DISCONNECTED": deadThreads++;  break;
                }
            }
            threadList.sort(Comparator.comparing(t -> (String) t.get("threadId")));
            wm.put("threads", threadList);
            workerList.add(wm);
        }
        workerList.sort(Comparator.comparing(w -> (String) w.get("workerId")));

        List<JobResult> sorted = new ArrayList<>(completedJobs.values());
        sorted.sort(Comparator.comparing(JobResult::getJobId).reversed());

        List<Map<String, Object>> recentJobs = new ArrayList<>();
        long totalOps = 0;
        long failCount = 0;
        long successCount = 0;
        for (JobResult r : sorted) {
            if (r.getSuccess()) { successCount++; totalOps += r.getOperationsCount(); }
            else { failCount++; }
            if (recentJobs.size() < 30) {
                Map<String, Object> rm = new LinkedHashMap<>();
                rm.put("jobId", r.getJobId());
                rm.put("workerId", r.getWorkerId());
                rm.put("threadId", r.getThreadId());
                rm.put("success", r.getSuccess());
                rm.put("errorMessage", r.getErrorMessage());
                rm.put("execTimeMs", r.getExecutionTimeMs());
                rm.put("opsCount", r.getOperationsCount());
                rm.put("outputSummary", r.getOutputSummary());
                recentJobs.add(rm);
            }
        }

        payload.put("serverTimeMs", System.currentTimeMillis());
        payload.put("jobsPending", jobQueue.size());
        payload.put("jobsCompleted", completedJobs.size());
        payload.put("jobsSucceeded", successCount);
        payload.put("jobsFailed", failCount);
        payload.put("totalOpsCompleted", totalOps);
        payload.put("exceptionsCount", totalExceptions.get());
        payload.put("networkDrops", totalNetworkDrops.get());
        payload.put("opsPerSec", currentOpsPerSec);
        payload.put("drainRatePerSec", currentDrainRate);
        payload.put("totalThreads", totalThreads);
        payload.put("busyThreads", busyThreads);
        payload.put("idleThreads", idleThreads);
        payload.put("deadThreads", deadThreads);
        payload.put("workers", workerList);
        payload.put("recentJobs", recentJobs);

        return payload;
    }

    static class TaskServiceImpl extends TaskServiceGrpc.TaskServiceImplBase {

        @Override
        public void registerWorker(WorkerRegistration req, StreamObserver<RegistrationAck> obs) {
            String wid = req.getWorkerId();
            int tc = Math.max(1, req.getThreadCount());

            WorkerNode node = workers.computeIfAbsent(wid, k -> new WorkerNode());
            node.workerId = wid;
            node.declaredThreadCount = tc;

            System.out.printf("[REGISTER] Worker '%s' joined with %d thread(s).%n", wid, tc);
            obs.onNext(RegistrationAck.newBuilder().setAccepted(true).setMessage("Accepted.").build());
            obs.onCompleted();
            broadcastTelemetry();
        }

        @Override
        public void sendHeartbeat(WorkerHeartbeat req, StreamObserver<HeartbeatAck> obs) {
            String wid = req.getWorkerId();
            String tid = req.getThreadId();
            String st = req.getStatus();

            WorkerNode node = workers.get(wid);
            if (node != null) {
                ThreadState ts = node.threads.computeIfAbsent(tid, k -> {
                    ThreadState n = new ThreadState();
                    n.threadId = tid;
                    n.workerId = wid;
                    return n;
                });
                boolean wasDisconnected = "DISCONNECTED".equals(ts.status);
                ts.lastHeartbeatMs = System.currentTimeMillis();
                ts.status = st.isEmpty() ? "IDLE" : st;
                if (wasDisconnected) {
                    System.out.printf("[RECONNECT] Thread %s back online.%n", tid);
                    broadcastTelemetry();
                }
            }

            obs.onNext(HeartbeatAck.newBuilder().setOk(true).build());
            obs.onCompleted();
        }

        @Override
        public void pollJob(PollRequest req, StreamObserver<JobAssignment> obs) {
            String wid = req.getWorkerId();
            String tid = req.getThreadId();

            WorkerNode node = workers.get(wid);
            if (node == null) {
                obs.onNext(JobAssignment.getDefaultInstance());
                obs.onCompleted();
                return;
            }

            ThreadState ts = node.threads.computeIfAbsent(tid, k -> {
                ThreadState n = new ThreadState();
                n.threadId = tid;
                n.workerId = wid;
                return n;
            });
            ts.lastHeartbeatMs = System.currentTimeMillis();

            if (!ts.currentJobId.isEmpty()) {
                obs.onNext(ts.currentJob != null ? ts.currentJob : JobAssignment.getDefaultInstance());
                obs.onCompleted();
                return;
            }

            if ("DISCONNECTED".equals(ts.status)) ts.status = "IDLE";

            JobAssignment job = jobQueue.poll();
            if (job != null) {
                ts.status = "BUSY";
                ts.currentJobId = job.getJobId();
                ts.currentJob = job;
                obs.onNext(job);
                broadcastTelemetry();
            } else {
                ts.status = "IDLE";
                obs.onNext(JobAssignment.getDefaultInstance());
            }
            obs.onCompleted();
        }

        @Override
        public void submitResult(JobResult req, StreamObserver<ResultAck> obs) {
            String wid = req.getWorkerId();
            String tid = req.getThreadId();

            WorkerNode node = workers.get(wid);
            if (node != null) {
                ThreadState ts = node.threads.get(tid);
                if (ts != null) {
                    ts.lastHeartbeatMs = System.currentTimeMillis();
                    ts.currentJob = null;
                    ts.currentJobId = "";
                    ts.status = "IDLE";
                }
            }

            completedJobs.put(req.getJobId(), req);
            opsInLastSecond.addAndGet(req.getOperationsCount());
            totalOpsCompleted.addAndGet(req.getOperationsCount());
            jobsInLastSecond.incrementAndGet();
            if (!req.getSuccess()) totalExceptions.incrementAndGet();

            obs.onNext(ResultAck.newBuilder().setOk(true).build());
            obs.onCompleted();
            broadcastTelemetry();
        }
    }

    static class ClusterWebSocketServer extends WebSocketServer {

        ClusterWebSocketServer(InetSocketAddress addr) {
            super(addr);
            setReuseAddr(true);
        }

        @Override
        public void onOpen(WebSocket conn, ClientHandshake hs) {
            System.out.println("[WS] Client connected: " + conn.getRemoteSocketAddress());
            try {
                conn.send(objectMapper.writeValueAsString(buildTelemetryPayload()));
            } catch (Exception e) {
                System.err.println("[WS] Initial push failed: " + e.getMessage());
            }
        }

        @Override
        public void onClose(WebSocket conn, int c, String r, boolean remote) {
            System.out.println("[WS] Client disconnected: " + conn.getRemoteSocketAddress());
        }

        @Override public void onMessage(WebSocket conn, String msg) {}

        @Override
        public void onError(WebSocket conn, Exception ex) {
            System.err.println("[WS] Error: " + ex.getMessage());
        }

        @Override
        public void onStart() {
            System.out.println("[WS] WebSocket server started.");
        }
    }
}
