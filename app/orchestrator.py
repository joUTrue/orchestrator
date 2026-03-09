# orchestrator.py
# Orchestrator 구현

import requests
import time
from app.redis_client import pop_job_queue, set_job_status

POSE_URL = "http://pose-model:8001/infer"
STT_URL = "http://stt-model:8002/infer"
GESTURE_URL = "http://gesture-model:8002/infer"
GAZE_URL = "http://gaze-model:8003/infer"


# 모델 실행
def run_model(url, payload):
    try:
        r = requests.post(url, json=payload, timeout=600)
        return r.json()
    except Exception as e:
        return {"error": str(e)}

# Orchestrator 메인 루프
def run_orchestrator():
    while True:
        job = pop_job_queue()

        if not job:
            continue

        job_id = job["job_id"]
        video_url = job["video_url"]

        print("START JOB", job_id)
        
        # 1. Pose 모델 실행
        set_job_status(job_id, {
            "state": "RUNNING",
            "step": "pose"
        })
        pose_result = run_model(POSE_URL, {
            "video_url": video_url
        })

        # 2. Gesture 모델 실행
        # set_job_status(job_id, {
        #     "state": "RUNNING",
        #     "step": "gesture"
        # })
        # gesture_result = run_model(GESTURE_URL, {
        #     "video_url": video_url
        # })

        # 3. Gaze 모델 실행
        # set_job_status(job_id, {
        #     "state": "RUNNING",
        #     "step": "gaze"
        # })
        # gaze_result = run_model(GAZE_URL, {
        #     "video_url": video_url
        # })
        
        # 4. STT 모델 실행
        # set_job_status(job_id, {
        #     "state": "RUNNING",
        #     "step": "stt"
        # })
        # stt_result = run_model(STT_URL, {
        #     "video_url": video_url
        # })
        
        # 완료
        set_job_status(job_id, {
            "state": "SUCCESS",
            "step": "done"
        })

        print("JOB DONE", job_id)