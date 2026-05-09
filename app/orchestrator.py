# orchestrator.py
# Orchestrator implementation

import json
import logging
import os
from typing import Any

import requests

from app.redis_client import pop_job_queue, set_job_status
from app.s3_uploader import S3ArtifactUploader, S3UploadConfigError

logger = logging.getLogger('app.orchestrator')

POSE_URL = os.getenv('POSE_URL', 'http://localhost:8001/infer')
STT_URL = os.getenv('STT_URL', 'http://localhost:8002/infer')
PITCH_URL = os.getenv('PITCH_URL', 'http://localhost:8005/infer')
REFINER_URL = os.getenv('REFINER_URL', 'http://localhost:8006/refine')
LLM_URL = os.getenv('LLM_URL', 'http://localhost:8004/feedback')
GESTURE_URL = os.getenv('GESTURE_URL', 'http://localhost:8004/infer')
GAZE_URL = os.getenv('GAZE_URL', 'http://localhost:8003/infer')
BACKEND_CALLBACK_URL = os.getenv('BACKEND_CALLBACK_URL')
MODEL_CONNECT_TIMEOUT = float(os.getenv('MODEL_CONNECT_TIMEOUT', '10'))
POSE_READ_TIMEOUT = float(os.getenv('POSE_READ_TIMEOUT', '1800'))
STT_READ_TIMEOUT = float(os.getenv('STT_READ_TIMEOUT', '600'))
PITCH_READ_TIMEOUT = float(os.getenv('PITCH_READ_TIMEOUT', '600'))
REFINER_READ_TIMEOUT = float(os.getenv('REFINER_READ_TIMEOUT', '120'))
LLM_READ_TIMEOUT = float(os.getenv('LLM_READ_TIMEOUT', '120'))
CALLBACK_CONNECT_TIMEOUT = float(os.getenv('CALLBACK_CONNECT_TIMEOUT', '5'))
CALLBACK_READ_TIMEOUT = float(os.getenv('CALLBACK_READ_TIMEOUT', '10'))
RESULT_UPLOADER = None


def run_model(url, payload, read_timeout):
    try:
        response = requests.post(
            url,
            json=payload,
            timeout=(MODEL_CONNECT_TIMEOUT, read_timeout),
        )
        response.raise_for_status()
        return response.json()
    except requests.HTTPError as e:
        detail = None
        response = getattr(e, 'response', None)
        if response is not None:
            try:
                data = response.json()
                detail = data.get('detail') or data.get('error') or json.dumps(data, ensure_ascii=False)
            except ValueError:
                detail = response.text.strip()

        if detail:
            logger.error('Model request failed for %s with status=%s detail=%s', url, response.status_code, detail)
            return {'error': f'{e} | detail: {detail}'}

        return {'error': str(e)}
    except Exception as e:
        return {'error': str(e)}


def _extract_error(result):
    if not isinstance(result, dict):
        return 'Invalid model response'

    if 'error' in result and result['error']:
        return str(result['error'])

    if 'detail' in result and result['detail']:
        return str(result['detail'])

    return None


def _post_job_status_callback(job_id, data):
    if not BACKEND_CALLBACK_URL:
        return

    payload = {
        'job_id': job_id,
        'state': data.get('state'),
        'step': data.get('step'),
        'result': data.get('result'),
    }

    if data.get('error'):
        payload['error'] = data['error']

    try:
        response = requests.post(
            BACKEND_CALLBACK_URL,
            json=payload,
            timeout=(CALLBACK_CONNECT_TIMEOUT, CALLBACK_READ_TIMEOUT),
        )
        response.raise_for_status()
    except Exception as e:
        logger.exception('Failed to send backend callback for job %s: %s', job_id, e)


def _serialize_status_value(value: Any) -> str:
    if value is None:
        return 'null'

    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)

    return str(value)


def _update_job_status(job_id, data):
    serialized = {key: _serialize_status_value(value) for key, value in data.items()}
    set_job_status(job_id, serialized)
    _post_job_status_callback(job_id, data)


def _fail_job(job_id, step, error):
    _update_job_status(job_id, {
        'state': 'FAILED',
        'step': step,
        'result': None,
        'error': error,
    })


def _set_step_running(job_id, step):
    _update_job_status(job_id, {
        'state': 'RUNNING',
        'step': step,
        'result': None,
        'error': None,
    })


def _set_step_end(job_id, step, result):
    _update_job_status(job_id, {
        'state': 'END',
        'step': step,
        'result': result,
        'error': None,
    })


def _get_result_uploader():
    global RESULT_UPLOADER

    if RESULT_UPLOADER is None:
        RESULT_UPLOADER = S3ArtifactUploader()

    return RESULT_UPLOADER


def _upload_model_outputs(job_id, model_name, result):
    uploader = _get_result_uploader()
    uploads = uploader.upload_model_outputs(job_id=job_id, model_name=model_name, result=result)
    logger.info('Uploaded %s artifacts for job %s: %s', model_name, job_id, uploads)
    return uploads


def _run_step(job_id, step_name, model_url, payload, read_timeout):
    _set_step_running(job_id, step_name)

    model_result = run_model(model_url, payload, read_timeout=read_timeout)
    step_error = _extract_error(model_result)
    if step_error:
        _fail_job(job_id, step_name, step_error)
        logger.error('JOB FAILED %s at %s: %s', job_id, step_name, step_error)
        return None

    try:
        uploaded_result = _upload_model_outputs(job_id, step_name, model_result)
    except S3UploadConfigError as e:
        _fail_job(job_id, step_name, str(e))
        logger.error('JOB FAILED %s at %s upload config: %s', job_id, step_name, e)
        return None
    except Exception as e:
        _fail_job(job_id, step_name, str(e))
        logger.exception('JOB FAILED %s while uploading %s result', job_id, step_name)
        return None

    _set_step_end(job_id, step_name, uploaded_result)
    return {
        'model_result': model_result,
        'uploaded_result': uploaded_result,
    }


# Orchestrator main loop
def run_orchestrator():
    while True:
        job = pop_job_queue()

        if not job:
            continue

        job_id = job['job_id']
        video_url = job['video_url']

        logger.info('START JOB %s', job_id)
        logger.info('Submitting pose request for video_url=%s', video_url)

        pose_execution = _run_step(
            job_id=job_id,
            step_name='pose',
            model_url=POSE_URL,
            payload={'video_url': video_url},
            read_timeout=POSE_READ_TIMEOUT,
        )
        if pose_execution is None:
            continue

        # gesture/gaze can be added here with the same pattern when enabled.

        stt_execution = _run_step(
            job_id=job_id,
            step_name='stt',
            model_url=STT_URL,
            payload={'video_url': video_url},
            read_timeout=STT_READ_TIMEOUT,
        )
        if stt_execution is None:
            continue

        pitch_execution = _run_step(
            job_id=job_id,
            step_name='pitch',
            model_url=PITCH_URL,
            payload={
                'video_url': video_url,
                'stt_result': stt_execution['model_result'],
            },
            read_timeout=PITCH_READ_TIMEOUT,
        )
        if pitch_execution is None:
            continue

        refiner_execution = _run_step(
            job_id=job_id,
            step_name='refiner',
            model_url=REFINER_URL,
            payload={
                'pitch_result': pitch_execution['model_result'],
                'stt_result': stt_execution['model_result'],
                'pose_result': pose_execution['model_result'],
            },
            read_timeout=REFINER_READ_TIMEOUT,
        )
        if refiner_execution is None:
            continue

        llm_execution = _run_step(
            job_id=job_id,
            step_name='llm',
            model_url=LLM_URL,
            payload={
                'job_id': job_id,
                'pose_result': pose_execution['model_result'],
                'stt_result': stt_execution['model_result'],
                'pitch_result': pitch_execution['model_result'],
                'refiner_result': refiner_execution['model_result'],
            },
            read_timeout=LLM_READ_TIMEOUT,
        )
        if llm_execution is None:
            continue

        logger.info('JOB %s completed successfully', job_id)
        logger.info('Pose result: %s', pose_execution['model_result'])
        logger.info('STT result: %s', stt_execution['model_result'])
        logger.info('Pitch result: %s', pitch_execution['model_result'])
        logger.info('Refiner result: %s', refiner_execution['model_result'])
        logger.info('LLM result: %s', llm_execution['model_result'])
        logger.info(
            'Uploaded artifacts: %s',
            {
                'pose': pose_execution['uploaded_result'],
                'stt': stt_execution['uploaded_result'],
                'pitch': pitch_execution['uploaded_result'],
                'refiner': refiner_execution['uploaded_result'],
                'llm': llm_execution['uploaded_result'],
            },
        )



