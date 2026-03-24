import json
import os
import time

import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_SOCKET_TIMEOUT = float(os.getenv("REDIS_SOCKET_TIMEOUT", "10"))
REDIS_CONNECT_TIMEOUT = float(os.getenv("REDIS_CONNECT_TIMEOUT", "5"))
REDIS_MAX_RETRIES = int(os.getenv("REDIS_MAX_RETRIES", "3"))
REDIS_RETRY_DELAY_SEC = float(os.getenv("REDIS_RETRY_DELAY_SEC", "0.5"))


def _build_client() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True,
        socket_timeout=REDIS_SOCKET_TIMEOUT,
        socket_connect_timeout=REDIS_CONNECT_TIMEOUT,
        health_check_interval=30,
        retry_on_timeout=True,
    )


redis_client = _build_client()


def _with_retry(fn):
    last_error = None

    for attempt in range(1, REDIS_MAX_RETRIES + 1):
        try:
            return fn()
        except (redis.ConnectionError, redis.TimeoutError, redis.BusyLoadingError) as e:
            last_error = e
            if attempt < REDIS_MAX_RETRIES:
                time.sleep(REDIS_RETRY_DELAY_SEC)

    raise last_error


def set_job_status(job_id, data):
    _with_retry(lambda: redis_client.hset(f"job:{job_id}:status", mapping=data))


def get_job_status(job_id):
    return _with_retry(lambda: redis_client.hgetall(f"job:{job_id}:status"))


def push_job_queue(job_data):
    payload = json.dumps(job_data)
    _with_retry(lambda: redis_client.lpush("job_queue", payload))


def pop_job_queue():
    # BRPOP timeout보다 socket_timeout이 짧으면 TimeoutError가 날 수 있으므로
    # 예외를 삼키고 None을 반환해 워커 스레드가 죽지 않게 한다.
    try:
        job = redis_client.brpop("job_queue", timeout=5)
        if job:
            return json.loads(job[1])
        return None
    except (redis.ConnectionError, redis.TimeoutError, redis.BusyLoadingError):
        time.sleep(REDIS_RETRY_DELAY_SEC)
        return None
