# models.py
# Pydantic 모델 정의
from pydantic import BaseModel

class JobRequest(BaseModel):
    video_url: str
    job_id: str