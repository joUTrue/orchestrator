# redis_client.py
# redis활용 job status 관리 및 job queue 구현

import redis
import json

redis_client = redis.Redis(
    host="redis",
    port=6379,
    decode_responses=True
)

def set_job_status(job_id, data):
    redis_client.hset(f"job:{job_id}:status", mapping=data)

def get_job_status(job_id):
    return redis_client.hgetall(f"job:{job_id}:status")

def push_job_queue(job_data):
    redis_client.lpush("job_queue", json.dumps(job_data))

def pop_job_queue():
    job = redis_client.brpop("job_queue")
    if job:
        return json.loads(job[1])
    return None