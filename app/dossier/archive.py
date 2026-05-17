from __future__ import annotations

from collections.abc import Mapping
from pathlib import PurePosixPath, PureWindowsPath

from .serialization import DOSSIER_STORE_LAYOUT_VERSION

ARCHIVE_MANIFEST_FILENAME = "archive_manifest.json"


def dossier_store_archive_manifest_to_dict(
    *,
    attachments_dir: str,
    files: list[dict[str, object]],
) -> dict[str, object]:
    normalized_attachments_dir = _validate_non_empty_revision_relative_path(
        attachments_dir,
        field_name="attachments_dir",
    )
    return {
        "store_layout_version": DOSSIER_STORE_LAYOUT_VERSION,
        "attachments_dir": normalized_attachments_dir,
        "files": _validate_archive_manifest_files(
            files,
            attachments_dir=normalized_attachments_dir,
            field_name="files",
        ),
    }


def dossier_store_archive_manifest_from_dict(data: Mapping[str, object]) -> dict[str, object]:
    normalized = _require_mapping(data, field_name="archive_manifest")
    version = _require_string(normalized, "store_layout_version", allow_empty=False)
    if version != DOSSIER_STORE_LAYOUT_VERSION:
        raise ValueError(f"Unsupported store_layout_version: {version}")
    attachments_dir = _validate_non_empty_revision_relative_path(
        _require_string(normalized, "attachments_dir", allow_empty=False),
        field_name="attachments_dir",
    )
    return {
        "store_layout_version": version,
        "attachments_dir": attachments_dir,
        "files": _validate_archive_manifest_files(
            _require_list(normalized, "files"),
            attachments_dir=attachments_dir,
            field_name="files",
        ),
    }


def _validate_archive_manifest_files(
    value: object,
    *,
    attachments_dir: str,
    field_name: str,
) -> list[dict[str, object]]:
    items = _validate_list(value, field_name=field_name)
    normalized_items: list[dict[str, object]] = []
    for index, item in enumerate(items):
        normalized_items.append(
            _validate_archive_manifest_file(
                _require_mapping(item, field_name=f"{field_name}[{index}]"),
                attachments_dir=attachments_dir,
                field_name=f"{field_name}[{index}]",
            )
        )
    return normalized_items


def _validate_archive_manifest_file(
    data: Mapping[str, object],
    *,
    attachments_dir: str,
    field_name: str,
) -> dict[str, object]:
    return {
        "dossier_file": validate_archive_attachment_dossier_file_path(
            _require_string(data, "dossier_file", allow_empty=False),
            attachments_dir=attachments_dir,
            field_name=f"{field_name}.dossier_file",
        ),
        "filename": _require_string(data, "filename", allow_empty=False),
        "checksum": _require_string(data, "checksum"),
        "size": _require_non_negative_int(data, "size"),
        "mime": _require_string(data, "mime"),
        "entry_kind": _require_string(data, "entry_kind"),
        "attachment_indices": _require_non_negative_int_list(data, "attachment_indices"),
        "document_indices": _require_non_negative_int_list(data, "document_indices"),
    }


def _require_mapping(value: object, *, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must be a mapping, got {type(value).__name__}")
    return value


def _require_string(data: Mapping[str, object], field_name: str, *, allow_empty: bool = True) -> str:
    if field_name not in data or data[field_name] is None:
        raise KeyError(f"Missing required field: {field_name}")
    return _validate_string(data[field_name], field_name=field_name, allow_empty=allow_empty)


def _validate_string(value: object, *, field_name: str, allow_empty: bool = True) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string, got {type(value).__name__}")
    if not value and not allow_empty:
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _require_list(data: Mapping[str, object], field_name: str) -> list[object]:
    if field_name not in data:
        raise KeyError(f"Missing required field: {field_name}")
    return _validate_list(data[field_name], field_name=field_name)


def _validate_list(value: object, *, field_name: str) -> list[object]:
    if not isinstance(value, list):
        raise TypeError(f"{field_name} must be a list, got {type(value).__name__}")
    return value


def _require_non_negative_int(data: Mapping[str, object], field_name: str) -> int:
    if field_name not in data or data[field_name] is None:
        raise KeyError(f"Missing required field: {field_name}")
    return _validate_non_negative_int(data[field_name], field_name=field_name)


def _validate_non_negative_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an int, got {type(value).__name__}")
    if value < 0:
        raise ValueError(f"{field_name} must be a non-negative int")
    return value


def _require_non_negative_int_list(data: Mapping[str, object], field_name: str) -> list[int]:
    if field_name not in data:
        raise KeyError(f"Missing required field: {field_name}")
    values = _validate_list(data[field_name], field_name=field_name)
    result: list[int] = []
    for index, value in enumerate(values):
        result.append(_validate_non_negative_int(value, field_name=f"{field_name}[{index}]"))
    return sorted(set(result))


def validate_archive_attachment_dossier_file_path(
    value: object,
    *,
    attachments_dir: str,
    field_name: str,
) -> str:
    normalized_attachments_dir = _validate_non_empty_revision_relative_path(
        attachments_dir,
        field_name="attachments_dir",
    )
    normalized = _validate_non_empty_revision_relative_path(value, field_name=field_name)
    attachments_parts = _split_revision_relative_path(normalized_attachments_dir)
    path_parts = _split_revision_relative_path(normalized)
    if path_parts[: len(attachments_parts)] != attachments_parts or len(path_parts) <= len(attachments_parts):
        raise ValueError(f"{field_name} must point to a file inside attachments_dir")
    return normalized


def _validate_non_empty_revision_relative_path(value: object, *, field_name: str) -> str:
    normalized = _validate_string(value, field_name=field_name, allow_empty=False)
    if _is_absolute_store_path(normalized):
        raise ValueError(f"{field_name} must be a relative path inside the revision root")
    parts = _split_revision_relative_path(normalized)
    if not parts:
        raise ValueError(f"{field_name} must be a relative path inside the revision root")
    if any(part == ".." for part in parts):
        raise ValueError(f"{field_name} must not escape the revision root")
    return "/".join(parts)


def _split_revision_relative_path(value: str) -> list[str]:
    return [part for part in value.replace("\\", "/").split("/") if part and part != "."]


def _is_absolute_store_path(value: str) -> bool:
    if value.startswith(("/", "\\")):
        return True
    windows_path = PureWindowsPath(value)
    return PurePosixPath(value).is_absolute() or windows_path.is_absolute() or bool(windows_path.drive)


__all__ = [
    "ARCHIVE_MANIFEST_FILENAME",
    "validate_archive_attachment_dossier_file_path",
    "dossier_store_archive_manifest_from_dict",
    "dossier_store_archive_manifest_to_dict",
]
