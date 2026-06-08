from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class ProofOfWorkResult:
    nonce: int
    result_hash: str
    execution_time_ms: int


def compute_pow(
    input_data: str,
    difficulty: int,
    *,
    start_nonce: int = 0,
    max_attempts: int | None = None,
) -> ProofOfWorkResult:
    """Find a nonce whose SHA256(input_data:nonce) hash has the target prefix."""
    if difficulty < 0 or difficulty > 64:
        raise ValueError("difficulty must be between 0 and 64")
    if start_nonce < 0:
        raise ValueError("start_nonce must be non-negative")
    if max_attempts is not None and max_attempts <= 0:
        raise ValueError("max_attempts must be positive when provided")

    target_prefix = "0" * difficulty
    nonce = start_nonce
    attempts = 0
    started = time.monotonic()

    while True:
        payload = f"{input_data}:{nonce}".encode("utf-8")
        digest = hashlib.sha256(payload).hexdigest()

        if digest.startswith(target_prefix):
            elapsed_ms = int((time.monotonic() - started) * 1000)
            return ProofOfWorkResult(
                nonce=nonce,
                result_hash=digest,
                execution_time_ms=elapsed_ms,
            )

        nonce += 1
        attempts += 1

        if max_attempts is not None and attempts >= max_attempts:
            raise RuntimeError("proof-of-work target was not found within max_attempts")

