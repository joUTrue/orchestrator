# orchestrator.py
# Orchestrator implementation

import os
import requests
from app.redis_client import pop_job_queue, set_job_status
import logging

logger = logging.getLogger("app.orchestrator")

POSE_URL = os.getenv("POSE_URL", "http://localhost:8001/infer")
STT_URL = os.getenv("STT_URL", "http://localhost:8002/infer")
GESTURE_URL = os.getenv("GESTURE_URL", "http://localhost:8004/infer")
GAZE_URL = os.getenv("GAZE_URL", "http://localhost:8003/infer")
BACKEND_CALLBACK_URL = os.getenv("BACKEND_CALLBACK_URL")
MODEL_CONNECT_TIMEOUT = float(os.getenv("MODEL_CONNECT_TIMEOUT", "10"))
POSE_READ_TIMEOUT = float(os.getenv("POSE_READ_TIMEOUT", "1800"))
STT_READ_TIMEOUT = float(os.getenv("STT_READ_TIMEOUT", "600"))
CALLBACK_CONNECT_TIMEOUT = float(os.getenv("CALLBACK_CONNECT_TIMEOUT", "5"))
CALLBACK_READ_TIMEOUT = float(os.getenv("CALLBACK_READ_TIMEOUT", "10"))


def run_model(url, payload, read_timeout):
    try:
        response = requests.post(
            url,
            json=payload,
            timeout=(MODEL_CONNECT_TIMEOUT, read_timeout),
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {"error": str(e)}


def _extract_error(result):
    if not isinstance(result, dict):
        return "Invalid model response"

    if "error" in result and result["error"]:
        return str(result["error"])

    if "detail" in result and result["detail"]:
        return str(result["detail"])

    return None


def _post_job_status_callback(job_id, data):
    if not BACKEND_CALLBACK_URL:
        return

    payload = {
        "job_id": job_id,
        "state": data.get("state"),
        "step": data.get("step"),
    }

    if data.get("error"):
        payload["error"] = data["error"]

    try:
        response = requests.post(
            BACKEND_CALLBACK_URL,
            json=payload,
            timeout=(CALLBACK_CONNECT_TIMEOUT, CALLBACK_READ_TIMEOUT),
        )
        response.raise_for_status()
    except Exception as e:
        logger.exception("Failed to send backend callback for job %s: %s", job_id, e)


def _update_job_status(job_id, data):
    set_job_status(job_id, data)
    _post_job_status_callback(job_id, data)


def _fail_job(job_id, step, error):
    _update_job_status(job_id, {
        "state": "FAILED",
        "step": step,
        "error": error,
    })


# Orchestrator main loop
def run_orchestrator():
    while True:
        job = pop_job_queue()

        if not job:
            continue

        job_id = job["job_id"]
        video_url = job["video_url"]

        logger.info("START JOB %s", job_id)
        logger.info("Submitting pose request for video_url=%s", video_url)
        
        # 1) Pose model
        _update_job_status(job_id, {
            "state": "RUNNING",
            "step": "pose",
        })
        pose_result = run_model(POSE_URL, {
            "video_url": video_url,
        }, read_timeout=POSE_READ_TIMEOUT)

        pose_error = _extract_error(pose_result)
        if pose_error:
            _fail_job(job_id, "pose", pose_error)
            logger.error("JOB FAILED %s at pose: %s", job_id, pose_error)
            continue

        # 2) Gesture model (not used yet)
        # _update_job_status(job_id, {
        #     "state": "RUNNING",
        #     "step": "gesture"
        # })
        # gesture_result = run_model(GESTURE_URL, {
        #     "video_url": video_url
        # }, read_timeout=POSE_READ_TIMEOUT)

        # 3) Gaze model (not used yet)
        # _update_job_status(job_id, {
        #     "state": "RUNNING",
        #     "step": "gaze"
        # })
        # gaze_result = run_model(GAZE_URL, {
        #     "video_url": video_url
        # }, read_timeout=POSE_READ_TIMEOUT)

        # 4) STT model
        _update_job_status(job_id, {
            "state": "RUNNING",
            "step": "stt",
        })
        stt_result = run_model(STT_URL, {
            "video_url": video_url,
        }, read_timeout=STT_READ_TIMEOUT)

        stt_error = _extract_error(stt_result)
        if stt_error:
            _fail_job(job_id, "stt", stt_error)
            logger.error("JOB FAILED %s at stt: %s", job_id, stt_error)
            continue

        # done
        _update_job_status(job_id, {
            "state": "SUCCESS",
            "step": "done",
        })
        
        logger.info("JOB %s completed successfully", job_id)
        logger.info("Pose result: %s", pose_result)
        logger.info("STT result: %s", stt_result)
