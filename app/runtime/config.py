from __future__ import annotations

import json
import os
from pathlib import Path


def _read_json_text(path: Path) -> dict[str, object] | None:
    for encoding in ("utf-8", "utf-8-sig"):
        try:
            payload = json.loads(path.read_text(encoding=encoding))
        except Exception:
            continue
        if isinstance(payload, dict):
            return payload
    return None


def _is_verified_rusprofile_profile(profile_path: Path) -> bool:
    payload_path = profile_path.parent / "rusprofile_auth_payload.json"
    if not payload_path.exists():
        return False
    payload = _read_json_text(payload_path)
    if not payload or not bool(payload.get("success")):
        return False
    parsed_company = payload.get("parsed_company")
    if not isinstance(parsed_company, dict):
        return False
    return bool(parsed_company.get("logged_in"))


def resolve_rusprofile_session_profile_file(*, raw_profile_file: str = "", cwd: Path | None = None) -> Path | None:
    explicit = raw_profile_file.strip()
    if explicit:
        explicit_path = Path(explicit).expanduser()
        return explicit_path if explicit_path.exists() else None

    env_profile = os.getenv("RUSPROFILE_SESSION_PROFILE_FILE", "").strip()
    if env_profile:
        env_path = Path(env_profile).expanduser()
        return env_path if env_path.exists() else None

    current_cwd = cwd or Path.cwd()
    runtime_root = current_cwd / "runtime_local"
    candidate_paths: list[Path] = []

    default_profile = runtime_root / "browser_sessions" / "rusprofile_session_profile.json"
    if default_profile.exists():
        candidate_paths.append(default_profile)

    for root_name in ("output", "output_test", "calibration_runs"):
        root = runtime_root / root_name
        if root.exists():
            candidate_paths.extend(root.rglob("rusprofile_session_profile.json"))

    unique_candidates = list({path.resolve() for path in candidate_paths if path.exists()})
    if not unique_candidates:
        return None

    def candidate_rank(path: Path) -> tuple[int, float]:
        verified = 1 if _is_verified_rusprofile_profile(path) else 0
        return verified, path.stat().st_mtime

    ranked = sorted(unique_candidates, key=candidate_rank, reverse=True)
    return ranked[0]


__all__ = ["resolve_rusprofile_session_profile_file"]
