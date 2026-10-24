"""Reads an uploaded save in chunks, refusing anything over the size cap."""

from __future__ import annotations

from fastapi import UploadFile

from app.core.errors import invalid_request

MAX_SAVE_BYTES = 12 * 1024 * 1024
_READ_CHUNK = 1024 * 1024


async def read_capped(file: UploadFile) -> bytes:
    """Read the upload without ever holding more than the cap in memory."""
    chunks: list[bytes] = []
    total = 0
    while chunk := await file.read(_READ_CHUNK):
        total += len(chunk)
        if total > MAX_SAVE_BYTES:
            # The error envelope, not a bare string: the bots parse one shape.
            raise invalid_request(
                f"Save file too large (limit {MAX_SAVE_BYTES // (1024 * 1024)} MiB)"
            )
        chunks.append(chunk)
    return b"".join(chunks)


__all__ = ["MAX_SAVE_BYTES", "read_capped"]
