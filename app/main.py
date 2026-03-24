import threading
from fastapi import FastAPI
from app.models import JobRequest
from app.redis_client import push_job_queue, get_job_status
from app.orchestrator import run_orchestrator
import logging

app = FastAPI()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

@app.on_event("startup")
def start_worker():
    thread = threading.Thread(target=run_orchestrator)
    thread.daemon = True
    thread.start()


@app.post("/job")
def create_job(req: JobRequest):

    job_data = {
        "job_id": req.job_id,
        "video_url": req.video_url
    }

    push_job_queue(job_data)

    return {
        "status": "queued",
        "job_id": req.job_id
    }


@app.get("/job/{job_id}")
def get_job(job_id: str):
    status = get_job_status(job_id)

    return {
        "job_id": job_id,
        "status": status
    }