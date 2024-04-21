import pytest
import asyncio_redis


def execute(loop, coro_func, *args, **kwargs):
    assert loop.run_until_complete(coro_func(*args, **kwargs)) is not None


@pytest.mark.benchmark(group="async-ping")
def benchmark_ping(benchmark, async_redis, loop):
    """Test the smallest and most simple PING command."""
    benchmark(execute, loop, async_redis.ping)


@pytest.mark.benchmark(group="async-get")
def benchmark_get(benchmark, async_redis, loop, key_get):
    """Test get from Redis single 1KiB string value."""
    benchmark(execute, loop, async_redis.get, key_get)


@pytest.mark.benchmark(group="async-hgetall")
def benchmark_hgetall(benchmark, async_redis, loop, key_hgetall):
    """Test get from hash few big items."""
    benchmark(execute, loop, async_redis.hgetall, key_hgetall)


@pytest.mark.benchmark(group="async-lrange")
def benchmark_lrange(benchmark, async_redis, loop, key_lrange):
    benchmark(execute, loop, async_redis.lrange, key_lrange, 0, -1)


@pytest.mark.benchmark(group="async-smembers")
def benchmark_smembers(benchmark, async_redis, loop, key_smembers):
    benchmark(execute, loop, async_redis.smembers, key_smembers)


@pytest.mark.benchmark(group="async-zrange")
def benchmark_zrange(benchmark, async_redis, loop, key_zrange):
    """Test get from sorted set 1k items."""
    # NOTE: asyncio_redis implies `withscores` parameter
    if isinstance(async_redis, asyncio_redis.Pool):
        kw = {}
    else:
        kw = {"withscores": True}
    benchmark(execute, loop, async_redis.zrange, key_zrange, 0, -1, **kw)
