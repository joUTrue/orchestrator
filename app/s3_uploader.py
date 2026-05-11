import json
import logging
import os
from pathlib import Path
from typing import Any

import boto3

logger = logging.getLogger("app.s3_uploader")

S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2"))
S3_KEY_PREFIX = os.getenv("S3_KEY_PREFIX", "orchestrator-results").strip("/")
SAVE_RESULTS_LOCAL = os.getenv("SAVE_RESULTS_LOCAL", "0").lower() in {"1", "true", "yes", "on"}
LOCAL_RESULTS_DIR = os.getenv("LOCAL_RESULTS_DIR", "/tmp/orchestrator-results")


class S3UploadConfigError(RuntimeError):
    pass


class S3ArtifactUploader:
    def __init__(self, bucket: str | None = None, region: str = AWS_REGION, key_prefix: str = S3_KEY_PREFIX):
        self.bucket = bucket or S3_BUCKET
        self.region = region
        self.key_prefix = key_prefix.strip("/")
        self.save_results_local = SAVE_RESULTS_LOCAL
        self.local_results_dir = Path(LOCAL_RESULTS_DIR)
        self.save_results_s3 = bool(self.bucket)

        if not self.save_results_s3 and not self.save_results_local:
            raise S3UploadConfigError("Neither S3 upload nor local result saving is configured")

        self.client = boto3.client("s3", region_name=self.region) if self.save_results_s3 else None

    def upload_model_outputs(self, job_id: str, model_name: str, result: dict[str, Any]) -> dict[str, Any]:
        result_json_payload = self._result_json_payload(result)
        uploaded = {
            "json_url": None,
            "mp4_urls": [],
            "json_artifacts": [],
            "local_artifacts": [],
        }

        if self.save_results_s3:
            uploaded["json_url"] = self.upload_json(
                payload=result_json_payload,
                key=self._build_key(job_id, model_name, "result.json"),
            )

            for file_path in self._collect_mp4_paths(result):
                key = self._build_key(job_id, model_name, f"media/{Path(file_path).name}")
                uploaded["mp4_urls"].append({
                    "source_path": file_path,
                    "url": self.upload_file(file_path=file_path, key=key, content_type="video/mp4"),
                })

            for artifact in self._collect_json_artifacts(result):
                key = self._build_key(job_id, model_name, f"artifacts/{artifact['filename']}")
                uploaded["json_artifacts"].append({
                    "name": artifact["name"],
                    "filename": artifact["filename"],
                    "url": self.upload_json(payload=artifact["payload"], key=key),
                })

        if self.save_results_local:
            uploaded["local_artifacts"] = self.save_local_outputs(
                job_id=job_id,
                model_name=model_name,
                result=result,
            )

        return uploaded

    def upload_json(self, payload: Any, key: str) -> str:
        if self.client is None or not self.bucket:
            raise S3UploadConfigError("S3 upload is not configured")

        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
        return self._build_url(key)

    def upload_file(self, file_path: str, key: str, content_type: str | None = None) -> str:
        if self.client is None or not self.bucket:
            raise S3UploadConfigError("S3 upload is not configured")

        with open(file_path, "rb") as file_obj:
            extra_args = {}
            if content_type:
                extra_args["ContentType"] = content_type

            self.client.upload_fileobj(file_obj, self.bucket, key, ExtraArgs=extra_args)

        return self._build_url(key)

    def save_local_outputs(self, job_id: str, model_name: str, result: dict[str, Any]) -> list[dict[str, Any]]:
        saved: list[dict[str, Any]] = []
        result_dir = self.local_results_dir / job_id.strip("/") / model_name.strip("/")
        (result_dir / "artifacts").mkdir(parents=True, exist_ok=True)
        (result_dir / "media").mkdir(parents=True, exist_ok=True)

        result_json_path = result_dir / "result.json"
        self._write_json_file(result_json_path, self._result_json_payload(result))
        saved.append({
            "type": "result_json",
            "path": str(result_json_path),
        })

        for artifact in self._collect_json_artifacts(result):
            artifact_path = result_dir / "artifacts" / artifact["filename"]
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            self._write_json_file(artifact_path, artifact["payload"])
            saved.append({
                "type": "json_artifact",
                "name": artifact["name"],
                "path": str(artifact_path),
            })

        for file_path in self._collect_mp4_paths(result):
            source_path = Path(file_path)
            target_path = result_dir / "media" / source_path.name
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_bytes(source_path.read_bytes())
            saved.append({
                "type": "media",
                "source_path": str(source_path),
                "path": str(target_path),
            })

        return saved

    def _build_key(self, job_id: str, model_name: str, artifact_name: str) -> str:
        parts = [self.key_prefix, job_id.strip("/"), model_name.strip("/"), artifact_name.strip("/")]
        return "/".join(part for part in parts if part)

    def _build_url(self, key: str) -> str:
        return f"https://{self.bucket}.s3.{self.region}.amazonaws.com/{key}"

    def _write_json_file(self, path: Path, payload: Any) -> None:
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _result_json_payload(self, result: dict[str, Any]) -> dict[str, Any]:
        return self._strip_artifact_payloads(result) if isinstance(result, dict) else result

    def _strip_artifact_payloads(self, value: Any) -> Any:
        if isinstance(value, dict):
            stripped: dict[str, Any] = {}
            for key, nested_value in value.items():
                if key == "artifacts" and isinstance(nested_value, dict):
                    stripped[key] = {
                        artifact_name: {
                            artifact_key: self._strip_artifact_payloads(artifact_value)
                            for artifact_key, artifact_value in artifact.items()
                            if artifact_key != "payload"
                        }
                        if isinstance(artifact, dict)
                        else self._strip_artifact_payloads(artifact)
                        for artifact_name, artifact in nested_value.items()
                    }
                else:
                    stripped[key] = self._strip_artifact_payloads(nested_value)
            return stripped

        if isinstance(value, list):
            return [self._strip_artifact_payloads(nested_value) for nested_value in value]

        return value

    def _collect_mp4_paths(self, value: Any) -> list[str]:
        collected: list[str] = []
        self._walk_for_mp4_paths(value, collected)

        deduped: list[str] = []
        seen: set[str] = set()
        for path in collected:
            if path not in seen:
                seen.add(path)
                deduped.append(path)
        return deduped

    def _walk_for_mp4_paths(self, value: Any, collected: list[str]) -> None:
        if isinstance(value, dict):
            for nested_value in value.values():
                self._walk_for_mp4_paths(nested_value, collected)
            return

        if isinstance(value, list):
            for nested_value in value:
                self._walk_for_mp4_paths(nested_value, collected)
            return

        if not isinstance(value, str):
            return

        if not value.lower().endswith(".mp4"):
            return

        candidate = Path(value)
        if not candidate.is_file():
            logger.info("Skipping non-local mp4 artifact path: %s", value)
            return

        collected.append(str(candidate))

    def _collect_json_artifacts(self, result: dict[str, Any]) -> list[dict[str, Any]]:
        artifacts = result.get("artifacts")
        if not isinstance(artifacts, dict):
            return []

        collected: list[dict[str, Any]] = []
        for name, artifact in artifacts.items():
            if not isinstance(artifact, dict):
                continue

            filename = artifact.get("filename")
            payload = artifact.get("payload")
            if not isinstance(filename, str) or not filename.strip() or payload is None:
                continue

            collected.append(
                {
                    "name": name,
                    "filename": filename.strip("/"),
                    "payload": payload,
                }
            )

        return collected
