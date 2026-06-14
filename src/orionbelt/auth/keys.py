"""Static API-key store with constant-time validation.

The store pre-builds a frozenset at startup; ``validate()`` is O(number of
keys) but uses ``secrets.compare_digest`` per key so a wrong key takes the
same time regardless of how many characters match (no timing side channel).
See design/PLAN_authentication.md §4.
"""

from __future__ import annotations

import secrets

# Reject keys with less entropy than this at startup (see §1 validation rules).
MIN_KEY_LENGTH = 16


def parse_keys(raw: str) -> frozenset[str]:
    """Split a comma-separated ``API_KEYS`` value into a deduped set.

    Whitespace is stripped and empty entries dropped. Duplicate keys are
    silently deduplicated.
    """
    return frozenset(k.strip() for k in raw.split(",") if k.strip())


def find_weak_keys(keys: frozenset[str]) -> list[str]:
    """Return masked prefixes of any keys shorter than ``MIN_KEY_LENGTH``.

    Only the first 4 characters are returned so error messages never leak a
    full key.
    """
    return [f"{k[:4]}..." for k in sorted(keys) if len(k) < MIN_KEY_LENGTH]


class KeyStore:
    """An immutable set of valid API keys."""

    def __init__(self, keys: frozenset[str]) -> None:
        self._keys = keys

    def __len__(self) -> int:
        return len(self._keys)

    @property
    def keys(self) -> frozenset[str]:
        """The configured keys. Used by SCRAM, which must know the secrets to
        verify a client proof (same trust boundary — the process holds them)."""
        return self._keys

    def validate(self, candidate: str) -> bool:
        """Return True when ``candidate`` matches a stored key (constant-time)."""
        if not candidate:
            return False
        candidate_bytes = candidate.encode("utf-8")
        # Iterate every key and compare_digest each one; never short-circuit
        # on a match so timing does not reveal which key (or how many) matched.
        matched = False
        for key in self._keys:
            if secrets.compare_digest(candidate_bytes, key.encode("utf-8")):
                matched = True
        return matched
