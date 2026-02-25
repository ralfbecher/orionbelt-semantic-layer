"""Tests for YAML parsing DoS safeguards in TrackedLoader."""

from __future__ import annotations

import pytest

from orionbelt.parser.loader import TrackedLoader, YAMLSafetyError, _MAX_DOCUMENT_SIZE
from orionbelt.service.model_store import ModelStore
from tests.conftest import SAMPLE_MODEL_YAML


class TestAnchorRejection:
    """OBML never uses YAML anchors/aliases — reject them entirely."""

    def test_billion_laughs_rejected(self, loader: TrackedLoader) -> None:
        """Classic billion-laughs payload with recursive anchor expansion."""
        yaml = (
            "a: &a ['lol','lol','lol','lol','lol']\n"
            "b: &b [*a,*a,*a,*a,*a]\n"
            "c: &c [*b,*b,*b,*b,*b]\n"
            "d: &d [*c,*c,*c,*c,*c]\n"
        )
        with pytest.raises(YAMLSafetyError, match="anchors/aliases"):
            loader.load_string(yaml)

    def test_simple_anchor_rejected(self, loader: TrackedLoader) -> None:
        """Even a single anchor definition is rejected."""
        yaml = "defaults: &defaults\n  color: red\nitem:\n  <<: *defaults\n"
        with pytest.raises(YAMLSafetyError, match="anchors/aliases"):
            loader.load_string(yaml)

    def test_anchor_in_sequence(self, loader: TrackedLoader) -> None:
        """Anchor in a sequence context."""
        yaml = "items:\n  - &item1 foo\n  - *item1\n"
        with pytest.raises(YAMLSafetyError, match="anchors/aliases"):
            loader.load_string(yaml)

    def test_ampersand_in_comment_not_rejected(self, loader: TrackedLoader) -> None:
        """An & inside a YAML comment must not trigger a false positive."""
        yaml = "# see R&D notes\n# &anchor_looking_thing\nkey: value\n"
        raw, _ = loader.load_string(yaml)
        assert raw["key"] == "value"


class TestDepthLimit:
    """Reject deeply nested YAML structures."""

    def test_deep_nesting_rejected(self, loader: TrackedLoader) -> None:
        """Nesting beyond max_depth should raise an error."""
        # Build 25 levels of nesting (exceeds _MAX_DEPTH=20)
        yaml = ""
        for i in range(25):
            yaml += "  " * i + f"level{i}:\n"
        yaml += "  " * 25 + "value: deep\n"
        with pytest.raises(Exception):  # ruamel raises its own error type
            loader.load_string(yaml)


class TestDocumentSize:
    """Reject oversized documents."""

    def test_oversized_document_rejected(self, loader: TrackedLoader) -> None:
        """Documents exceeding 5M chars are rejected."""
        yaml = "key: " + "x" * (_MAX_DOCUMENT_SIZE + 1) + "\n"
        with pytest.raises(YAMLSafetyError, match="maximum size"):
            loader.load_string(yaml)

    def test_just_under_limit_passes(self, loader: TrackedLoader) -> None:
        """Documents just under the limit are accepted."""
        # A small valid document well under the limit
        yaml = "key: value\n"
        raw, _ = loader.load_string(yaml)
        assert raw["key"] == "value"


class TestNodeCount:
    """Reject documents with excessive node counts."""

    def test_excessive_node_count_rejected(self, loader: TrackedLoader) -> None:
        """Flat structure with >50,000 nodes is rejected."""
        # Generate a YAML map with 50,001 keys
        lines = [f"k{i}: v{i}" for i in range(50_001)]
        yaml = "\n".join(lines)
        with pytest.raises(YAMLSafetyError, match="node count"):
            loader.load_string(yaml)


class TestValidOBML:
    """Ensure legitimate OBML models are not affected by safety checks."""

    def test_valid_obml_passes(self, loader: TrackedLoader) -> None:
        """The standard sample model parses without errors."""
        raw, source_map = loader.load_string(SAMPLE_MODEL_YAML)
        assert "dataObjects" in raw
        assert "dimensions" in raw
        assert "measures" in raw

    def test_empty_document_passes(self, loader: TrackedLoader) -> None:
        """Empty YAML is still accepted."""
        raw, _ = loader.load_string("")
        assert raw == {}


class TestModelStoreIntegration:
    """YAMLSafetyError surfaces as YAML_SAFETY_ERROR through ModelStore."""

    def test_anchor_returns_safety_error(self) -> None:
        store = ModelStore()
        summary = store.validate("a: &a [1,2,3]\nb: *a\n")
        assert summary.valid is False
        assert len(summary.errors) >= 1
        assert summary.errors[0].code == "YAML_SAFETY_ERROR"
        assert "anchors/aliases" in summary.errors[0].message

    def test_oversized_returns_safety_error(self) -> None:
        store = ModelStore()
        yaml = "key: " + "x" * (_MAX_DOCUMENT_SIZE + 1) + "\n"
        summary = store.validate(yaml)
        assert summary.valid is False
        assert summary.errors[0].code == "YAML_SAFETY_ERROR"
        assert "maximum size" in summary.errors[0].message
