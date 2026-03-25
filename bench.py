import asyncio
import time
import uuid
import statistics
from typing import List


HOST = "127.0.0.1"
PORT = 8080


# ---------- HTTP ----------

async def http_request(request: bytes) -> tuple[float, int]:
    reader, writer = await asyncio.open_connection(HOST, PORT)

    start = time.perf_counter()

    writer.write(request)
    await writer.drain()

    total_bytes = 0
    while True:
        chunk = await reader.read(65536)
        if not chunk:
            break
        total_bytes += len(chunk)

    end = time.perf_counter()

    writer.close()
    await writer.wait_closed()

    return end - start, total_bytes


# ---------- REQUEST BUILDERS ----------

def make_post(payload: bytes) -> bytes:
    return (
        f"POST /records HTTP/1.1\r\n"
        "Host: localhost\r\n"
        f"Content-Length: {len(payload)}\r\n"
        "Connection: close\r\n"
        "\r\n"
    ).encode() + payload


def make_get(key: str) -> bytes:
    return (
        f"GET /records/{key} HTTP/1.1\r\n"
        "Host: localhost\r\n"
        "\r\n"
    ).encode()


def make_dump() -> bytes:
    return (
        "GET /dump HTTP/1.1\r\n"
        "Host: localhost\r\n"
        "\r\n"
    ).encode()


# ---------- WORKERS ----------

async def worker_write(n: int, latencies: List[float], keys: List[uuid.UUID]) -> None:
    for _ in range(n):
        key = uuid.uuid4()
        payload = key.bytes + b"x" * 100  # 100 bytes

        req = make_post(payload)
        latency, _ = await http_request(req)

        latencies.append(latency)
        keys.append(key)


async def worker_read(n: int, latencies: List[float], keys: List[uuid.UUID]) -> None:
    for i in range(n):
        key = keys[i % len(keys)]
        req = make_get(str(key))

        latency, _ = await http_request(req)
        latencies.append(latency)


async def worker_dump(n: int, latencies: List[float], sizes: List[int]) -> None:
    req = make_dump()

    for _ in range(n):
        latency, size = await http_request(req)
        latencies.append(latency)
        sizes.append(size)


# ---------- BENCH ----------

def print_stats(name: str, latencies: List[float], total: int, duration: float) -> None:
    lat_sorted = sorted(latencies)

    def pct(p: float) -> float:
        return lat_sorted[int(len(lat_sorted) * p)]

    print(f"\n=== {name} ===")
    print(f"Requests: {total}")
    print(f"Time: {duration:.2f}s")
    print(f"RPS: {total / duration:.0f}")

    print("Latency:")
    print(f"avg: {statistics.mean(latencies)*1000:.2f} ms")
    print(f"p50: {pct(0.5)*1000:.2f} ms")
    print(f"p95: {pct(0.95)*1000:.2f} ms")


async def bench_write(total: int, concurrency: int) -> List[uuid.UUID]:
    latencies: List[float] = []
    keys: List[str] = []

    per_worker = total // concurrency

    start = time.perf_counter()

    await asyncio.gather(*[
        worker_write(per_worker, latencies, keys)
        for _ in range(concurrency)
    ])

    duration = time.perf_counter() - start

    print_stats("WRITE", latencies, total, duration)

    return keys


async def bench_read(total: int, concurrency: int, keys: List[uuid.UUID]) -> None:
    latencies: List[float] = []

    per_worker = total // concurrency

    start = time.perf_counter()

    await asyncio.gather(*[
        worker_read(per_worker, latencies, keys)
        for _ in range(concurrency)
    ])

    duration = time.perf_counter() - start

    print_stats("READ", latencies, total, duration)


async def bench_dump(total: int, concurrency: int) -> None:
    latencies: List[float] = []
    sizes: List[int] = []

    per_worker = total // concurrency

    start = time.perf_counter()

    await asyncio.gather(*[
        worker_dump(per_worker, latencies, sizes)
        for _ in range(concurrency)
    ])

    duration = time.perf_counter() - start

    # latency stats
    lat_sorted = sorted(latencies)

    def pct(p: float) -> float:
        return lat_sorted[int(len(lat_sorted) * p)]

    total_bytes = sum(sizes)

    print("\n=== DUMP ===")
    print(f"Requests: {total}")
    print(f"Time: {duration:.2f}s")
    print(f"RPS: {total / duration:.0f}")

    print("\nLatency:")
    print(f"avg: {statistics.mean(latencies)*1000:.2f} ms")
    print(f"p50: {pct(0.5)*1000:.2f} ms")
    print(f"p95: {pct(0.95)*1000:.2f} ms")

    print("\nThroughput:")
    print(f"Total: {total_bytes / (1024*1024):.2f} MB")
    print(f"MB/s: {total_bytes / duration / (1024*1024):.2f}")


# ---------- ENTRY ----------

async def main() -> None:
    total = 10000
    concurrency = 10

    print("Running WRITE benchmark...")
    keys = await bench_write(total, concurrency)

    print("\nRunning READ benchmark...")
    await bench_read(total, concurrency, keys)

    print("\nRunning DUMP benchmark...")
    await bench_dump(
        total=1000,  # меньше, потому что dump тяжёлый
        concurrency=10
    )


if __name__ == "__main__":
    asyncio.run(main())