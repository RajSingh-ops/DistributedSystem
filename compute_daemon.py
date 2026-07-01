import argparse
import array
import hashlib
import json
import random
import socket
import string
import sys
import traceback


def job_hash_cracking(complexity, seed):
    data = f"seed-{seed}".encode("utf-8")
    for _ in range(complexity):
        data = hashlib.sha256(data).digest()
    return {
        "operations_count": complexity,
        "output_summary": f"Chain depth {complexity:,} -> {data.hex()[:16]}..."
    }


def job_log_parsing(complexity, seed):
    rng = random.Random(seed)
    keywords = ["ERROR", "WARN", "INFO", "DEBUG", "CRITICAL", "TIMEOUT"]
    lines = []
    for _ in range(complexity):
        level = rng.choice(keywords)
        ts = f"2025-01-01T{rng.randint(0,23):02d}:{rng.randint(0,59):02d}:{rng.randint(0,59):02d}Z"
        msg = "".join(rng.choices(string.ascii_lowercase + " ", k=rng.randint(10, 40)))
        lines.append(f"[{ts}] [{level}] {msg}")
    log_blob = "\n".join(lines)
    counts = {kw: log_blob.count(kw) for kw in keywords}
    top_kw = max(counts, key=counts.get)
    total_chars = len(log_blob)
    return {
        "operations_count": total_chars,
        "output_summary": f"Scanned {total_chars:,} chars | Top: {top_kw}={counts[top_kw]:,}"
    }


def job_sort_benchmark(complexity, seed):
    rng = random.Random(seed)
    n = min(complexity, 5_000_000)
    arr = array.array('l', (rng.randint(-10_000_000, 10_000_000) for _ in range(n)))
    arr_list = arr.tolist()
    arr_list.sort()
    checksum = arr_list[0] + arr_list[-1]
    return {
        "operations_count": n,
        "output_summary": f"Sorted {n:,} integers | Checksum(first+last): {checksum:,}"
    }


def job_prime_sieve(complexity, seed):
    limit = min(complexity, 10_000_000)
    sieve = bytearray([1]) * (limit + 1)
    sieve[0] = sieve[1] = 0
    for i in range(2, int(limit ** 0.5) + 1):
        if sieve[i]:
            sieve[i * i::i] = bytearray(len(sieve[i * i::i]))
    prime_count = sum(sieve)
    largest_prime = next(i for i in range(limit, 1, -1) if sieve[i])
    return {
        "operations_count": limit,
        "output_summary": f"Sieve({limit:,}) -> {prime_count:,} primes | Largest: {largest_prime:,}"
    }


JOB_REGISTRY = {
    "HASH_CRACKING":  job_hash_cracking,
    "LOG_PARSING":    job_log_parsing,
    "SORT_BENCHMARK": job_sort_benchmark,
    "PRIME_SIEVE":    job_prime_sieve,
}


def dispatch_job(request):
    job_id = request.get("job_id", "UNKNOWN")
    job_type = request.get("job_type", "UNKNOWN").upper()
    complexity = int(request.get("complexity", 10_000))

    payload_raw = request.get("payload_json", "{}")
    try:
        payload = json.loads(payload_raw) if payload_raw else {}
    except Exception:
        payload = {}
    seed = int(payload.get("seed", 42))

    handler = JOB_REGISTRY.get(job_type)
    if handler is None:
        return {
            "job_id": job_id,
            "success": False,
            "error_message": f"Unknown job type: '{job_type}'",
            "operations_count": 0,
            "output_summary": ""
        }

    result = handler(complexity, seed)
    return {
        "job_id": job_id,
        "success": True,
        "error_message": "",
        "operations_count": result["operations_count"],
        "output_summary": result["output_summary"]
    }


def run_daemon(port):
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    server_sock.bind(("127.0.0.1", port))
    server_sock.listen(1)

    print(f"[compute_daemon] Listening on TCP 127.0.0.1:{port}", flush=True)

    conn, addr = server_sock.accept()
    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    print(f"[compute_daemon] Connected from {addr}", flush=True)

    buffer = ""
    try:
        while True:
            chunk = conn.recv(65536)
            if not chunk:
                break

            buffer += chunk.decode("utf-8")

            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                line = line.strip()
                if not line:
                    continue

                try:
                    request = json.loads(line)
                except json.JSONDecodeError as e:
                    response = {
                        "job_id": "PARSE_ERROR",
                        "success": False,
                        "error_message": f"JSON parse error: {e}",
                        "operations_count": 0,
                        "output_summary": ""
                    }
                    conn.sendall((json.dumps(response) + "\n").encode("utf-8"))
                    continue

                try:
                    response = dispatch_job(request)
                except MemoryError:
                    response = {
                        "job_id": request.get("job_id", "UNKNOWN"),
                        "success": False,
                        "error_message": "MemoryError during computation.",
                        "operations_count": 0,
                        "output_summary": ""
                    }
                except Exception:
                    response = {
                        "job_id": request.get("job_id", "UNKNOWN"),
                        "success": False,
                        "error_message": traceback.format_exc().strip(),
                        "operations_count": 0,
                        "output_summary": ""
                    }

                conn.sendall((json.dumps(response) + "\n").encode("utf-8"))

    except Exception as fatal:
        sys.stderr.write(f"[compute_daemon] Fatal: {fatal}\n")
        sys.stderr.flush()
    finally:
        conn.close()
        server_sock.close()
        print(f"[compute_daemon] Port {port} shut down.", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    run_daemon(args.port)


if __name__ == "__main__":
    main()
