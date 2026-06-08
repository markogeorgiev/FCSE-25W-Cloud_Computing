from __future__ import annotations

import hashlib

from app.pow import compute_pow


def test_compute_pow_returns_hash_matching_difficulty() -> None:
    result = compute_pow("unit-test-payload", 2)

    assert result.result_hash.startswith("00")
    assert result.nonce >= 0
    assert result.execution_time_ms >= 0


def test_compute_pow_hash_uses_input_colon_nonce_format() -> None:
    result = compute_pow("payload", 1)
    expected_hash = hashlib.sha256(f"payload:{result.nonce}".encode("utf-8")).hexdigest()

    assert result.result_hash == expected_hash

