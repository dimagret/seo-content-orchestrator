"""Reproducible Stage B release benchmark and fail-closed verdict contract."""

from __future__ import annotations

import copy
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

import pytest

from seo_orchestrator.canonical import sha256_fingerprint

_ROOT = Path(__file__).parents[2]
_MANIFEST_PATH = _ROOT / "fixtures/benchmark/manifest.json"
_VERDICT_PATH = _ROOT / "docs/evidence/stage-b-release-verdict.md"
_REQUIRED_CATEGORIES = {
    "local_automotive_commercial_service",
    "b2b_complex_service",
    "consumer_confectionery_page",
    "different_locale_language",
}
_METRICS = {
    "required_section_coverage",
    "factual_support",
    "source_traceability",
    "keyword_naturalness",
    "keyword_overuse",
    "internal_links",
    "language_locale_compliance",
    "meta_schema",
    "human_editorial_score",
}
_WEIGHTS = {
    "required_section_coverage": 15,
    "factual_support": 15,
    "source_traceability": 15,
    "keyword_naturalness": 10,
    "keyword_overuse": 10,
    "internal_links": 5,
    "language_locale_compliance": 10,
    "meta_schema": 5,
    "human_editorial_score": 15,
}
_THRESHOLD_METRICS = {
    "required_section_coverage_minimum": "required_section_coverage",
    "factual_support_minimum": "factual_support",
    "source_traceability_minimum": "source_traceability",
    "language_locale_compliance_minimum": "language_locale_compliance",
    "human_editorial_score_minimum": "human_editorial_score",
}
_METRIC_DEFINITIONS_SHA256 = "45b4d7f752c8c03791b02229235e7188dbd71f88d3b2beb1944b52bb5fc22664"


def _manifest() -> dict[str, Any]:
    value = json.loads(_MANIFEST_PATH.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _derive_contamination_tokens(case: dict[str, Any]) -> dict[str, list[str]]:
    context = case["compiled_context"]
    company = context["company"]
    direction = context["direction"]
    audience = context["audience"]
    brief = context["brief"]
    markers = sorted(set(re.findall(r"marker-[a-z]+-[a-z]+", json.dumps(context))))
    return {
        "identities": [
            company["company_id"],
            company["name"],
            direction["direction_id"],
            direction["name"],
            audience["audience_segment_id"],
            audience["name"],
        ],
        "offers": direction["offerings"],
        "geographies": [company["service_geography"], audience["geography"]],
        "urls": [*brief["competitor_urls"], *direction["internal_link_catalog"]],
        "case_references": [
            *company["case_references"],
            *direction["direction_cases"],
        ],
        "markers": markers,
    }


def _assert_no_cross_case_contamination(cases: list[dict[str, Any]]) -> None:
    tokens_by_case: dict[str, set[str]] = {}
    for case in cases:
        token_groups = case["contamination_tokens"]
        assert token_groups == _derive_contamination_tokens(case)
        assert case["unique_contamination_markers"] == token_groups["markers"]
        tokens = {
            token
            for group in token_groups.values()
            for token in group
        }
        assert tokens
        assert all(isinstance(token, str) and token.strip() == token for token in tokens)
        assert "" not in tokens
        tokens_by_case[case["case_id"]] = tokens

    for case in cases:
        own_text = json.dumps(case["compiled_context"], ensure_ascii=False, sort_keys=True)
        assert tokens_by_case[case["case_id"]] <= {
            token for token in tokens_by_case[case["case_id"]] if token in own_text
        }
        for other_id, foreign_tokens in tokens_by_case.items():
            if other_id == case["case_id"]:
                continue
            for token in foreign_tokens:
                assert token not in own_text


def _assert_prohibited_claim_authority(cases: list[dict[str, Any]]) -> None:
    for case in cases:
        claims = case["prohibited_claims"]
        context = case["compiled_context"]
        frozen_forbidden = {
            *context["company"]["forbidden_claims"],
            *context["direction"]["forbidden_claims"],
        }
        assert claims
        assert len(claims) == len(set(claims))
        assert all(isinstance(claim, str) and claim.strip() == claim for claim in claims)
        assert "" not in claims
        assert set(claims) == frozen_forbidden


def _derive_release_verdict(manifest: dict[str, Any]) -> str:
    if manifest["mandatory_release_blockers"]:
        return "REJECTED"
    thresholds = manifest["evaluation_contract"]["acceptance_thresholds"]
    for case in manifest["cases"]:
        weights = case["evaluator_rubric"]
        for result in case["results"].values():
            if result["status"] != "complete":
                return "REJECTED"
            metrics = result["metrics"]
            if any(
                type(score) not in (int, float) or not 0 <= score <= 100
                for score in metrics.values()
            ):
                return "REJECTED"
            weighted = sum(metrics[name] * weights[name] for name in _METRICS) / 100
            if weighted < thresholds["weighted_score_minimum"]:
                return "REJECTED"
            if any(
                metrics[metric] < thresholds[threshold]
                for threshold, metric in _THRESHOLD_METRICS.items()
            ):
                return "REJECTED"
            if (
                result["prohibited_claim_occurrences"]
                > thresholds["prohibited_claim_occurrences_maximum"]
            ):
                return "REJECTED"
            if (
                result["cross_company_contamination_occurrences"]
                > thresholds["cross_company_contamination_occurrences_maximum"]
            ):
                return "REJECTED"
    return "APPROVED_FOR_SINGLE_USER_STAGE_B"


def test_manifest_freezes_four_required_benchmark_categories() -> None:
    manifest = _manifest()
    cases = manifest["cases"]

    assert manifest["schema_version"] == 1
    assert manifest["candidate_head"] == "8b61ca38d8de66f2da68b206f1f441f4be0bf68e"
    assert 3 <= len(cases) <= 5
    assert {case["category"] for case in cases} == _REQUIRED_CATEGORIES
    assert len({case["case_id"] for case in cases}) == len(cases)
    assert len({case["snapshot_hash"] for case in cases}) == len(cases)


def test_manifest_self_integrity_is_reproducible() -> None:
    manifest = _manifest()
    integrity = manifest.pop("integrity")

    assert integrity == {
        "algorithm": "sha256",
        "canonicalization": "seo_orchestrator.canonical.canonical_json UTF-8 over the top-level object with integrity omitted",
        "sha256": sha256_fingerprint(manifest),
    }


def test_snapshot_hashes_and_version_bindings_are_canonical() -> None:
    for case in _manifest()["cases"]:
        context = case["compiled_context"]
        company = context["company"]
        direction = context["direction"]
        audience = context["audience"]
        brief = context["brief"]

        assert case["snapshot_hash"] == sha256_fingerprint(context)
        assert context["schema_version"] == 1
        assert case["prompt_set_version"] == context["prompt_set_version"]
        assert case["company_id"] == company["company_id"] == brief["company_id"]
        assert case["company_profile_version"] == company["company_profile_version"]
        assert case["company_profile_version"] == brief["company_profile_version"]
        assert case["direction_id"] == direction["direction_id"] == brief["direction_id"]
        assert case["direction_version"] == direction["direction_version"]
        assert case["direction_version"] == brief["direction_version"]
        assert case["audience_segment_id"] == audience["audience_segment_id"]
        assert case["audience_segment_id"] == brief["audience_segment_id"]
        assert case["audience_version"] == audience["audience_version"]
        assert case["audience_version"] == brief["audience_version"]
        assert case["brief_id"] == brief["brief_id"]
        assert case["expected_required_sections"] == brief["page_structure"]
    _assert_prohibited_claim_authority(_manifest()["cases"])


def test_rubric_and_equivalent_path_result_shape_are_complete() -> None:
    manifest = _manifest()
    contract = manifest["evaluation_contract"]
    assert contract["rubric_version"] == 1
    assert contract["scale"] == {"minimum": 0, "maximum": 100}
    assert set(contract["metric_definitions"]) == _METRICS
    assert sha256_fingerprint(contract["metric_definitions"]) == _METRIC_DEFINITIONS_SHA256
    for definition in contract["metric_definitions"].values():
        assert set(definition) == {"measurement", "scoring", "evaluator"}
        assert len(definition["measurement"]) >= 60
        assert len(definition["scoring"]) >= 25
        assert definition["evaluator"] in {
            "deterministic",
            "two-independent-human-reviewers",
            "deterministic-plus-human-confirmation",
        }
    assert contract["review_protocol"] == {
        "reviewer_count": 2,
        "blind_to_execution_path": True,
        "rating_anchors": {
            "1": "unusable",
            "2": "major rewrite",
            "3": "substantial edit",
            "4": "minor edit",
            "5": "publishable",
        },
        "disagreement_rule": "A third reviewer adjudicates when metric scores differ by more than 20 points.",
        "weighted_score_formula": "sum(metric_score * metric_weight) / 100",
        "approval_formula": "approve only when both paths are complete, no mandatory blocker exists, weighted score and every hard threshold pass, and prohibited-claim and contamination counts are zero",
    }
    assert contract["acceptance_thresholds"] == {
        "weighted_score_minimum": 80,
        "required_section_coverage_minimum": 100,
        "factual_support_minimum": 90,
        "source_traceability_minimum": 90,
        "language_locale_compliance_minimum": 90,
        "human_editorial_score_minimum": 80,
        "prohibited_claim_occurrences_maximum": 0,
        "cross_company_contamination_occurrences_maximum": 0,
        "both_paths_must_be_complete": True,
    }
    for case in manifest["cases"]:
        assert case["evaluator_rubric"] == _WEIGHTS
        for path_name in ("source_path", "universal_path"):
            result = case["results"][path_name]
            assert set(result["metrics"]) == _METRICS
            assert result["status"] == "not_run"
            assert set(result["metrics"].values()) == {None}
            assert result["paid_call_count"] == 0
            assert result["retry_count"] == 0
            assert result["runtime_ms"] is None
            assert result["prohibited_claim_occurrences"] is None
            assert result["cross_company_contamination_occurrences"] is None
            assert result["deviation"]


def test_frozen_inputs_have_no_cross_case_identity_or_marker_contamination() -> None:
    cases = _manifest()["cases"]
    _assert_no_cross_case_contamination(cases)


@pytest.mark.parametrize(
    ("token_group", "target_path"),
    [
        ("offers", ("direction", "offerings")),
        ("geographies", ("company", "service_geography")),
        ("urls", ("brief", "competitor_urls")),
        ("case_references", ("company", "case_references")),
    ],
)
def test_contamination_mutations_fail_even_with_recomputed_snapshot_hash(
    token_group: str,
    target_path: tuple[str, str],
) -> None:
    cases = copy.deepcopy(_manifest()["cases"])
    foreign = cases[1]["contamination_tokens"][token_group][0]
    record, field = target_path
    current = cases[0]["compiled_context"][record][field]
    if isinstance(current, list):
        current.append(foreign)
    else:
        cases[0]["compiled_context"][record][field] = foreign
    cases[0]["snapshot_hash"] = sha256_fingerprint(cases[0]["compiled_context"])

    with pytest.raises(AssertionError):
        _assert_no_cross_case_contamination(cases)


@pytest.mark.parametrize("token_group", ["identities", "offers", "geographies", "urls", "case_references", "markers"])
def test_omitting_authoritative_contamination_tokens_fails(token_group: str) -> None:
    cases = copy.deepcopy(_manifest()["cases"])
    cases[0]["contamination_tokens"][token_group].pop()

    with pytest.raises(AssertionError):
        _assert_no_cross_case_contamination(cases)


def test_vacuous_prohibited_claim_mutation_fails() -> None:
    cases = copy.deepcopy(_manifest()["cases"])
    cases[0]["prohibited_claims"] = [""]

    with pytest.raises(AssertionError):
        _assert_prohibited_claim_authority(cases)


def test_omitting_frozen_prohibited_claim_fails() -> None:
    cases = copy.deepcopy(_manifest()["cases"])
    cases[0]["prohibited_claims"].pop()

    with pytest.raises(AssertionError):
        _assert_prohibited_claim_authority(cases)


def test_russian_case_is_materially_russian_not_just_locale_labeled() -> None:
    case = next(case for case in _manifest()["cases"] if case["case_id"] == "automotive-ru")
    context = case["compiled_context"]
    text = json.dumps(context, ensure_ascii=False)
    cyrillic = len(re.findall(r"[А-Яа-яЁё]", text))
    latin = len(re.findall(r"[A-Za-z]", text))
    strings: list[str] = []

    def collect(value: Any) -> None:
        if isinstance(value, str):
            strings.append(value)
        elif isinstance(value, list):
            for item in value:
                collect(item)
        elif isinstance(value, dict):
            for item in value.values():
                collect(item)

    collect(context)

    assert context["brief"]["target_language"] == "ru"
    assert context["brief"]["locale"] == "ru-RU"
    assert cyrillic >= 1_500
    assert cyrillic / (cyrillic + latin) >= 0.40
    assert max(Counter(strings).values()) <= 6
    assert len({value for value in strings if re.search(r"[А-Яа-яЁё]", value)}) >= 45
    assert len(context["direction"]["offerings"]) >= 4
    assert len(context["audience"]["pains_and_risks"]) >= 3
    assert len(context["company"]["proof_points"]) >= 2


def test_release_is_rejected_when_mandatory_evidence_is_missing() -> None:
    manifest = _manifest()
    blockers = {item["id"] for item in manifest["mandatory_release_blockers"]}
    all_paths_complete = all(
        result["status"] == "complete"
        for case in manifest["cases"]
        for result in case["results"].values()
    )

    assert blockers == {
        "task19-oci-boundary",
        "equivalent-path-benchmark",
        "universal-cloud-readiness",
    }
    assert not all_paths_complete
    assert manifest["verdict"] == "REJECTED"
    assert _derive_release_verdict(manifest) == manifest["verdict"]
    assert manifest["external_costs"] == {
        "paid_calls": 0,
        "amount": 0,
        "currency": None,
    }
    assert set(manifest["mutations"].values()) == {False}


def test_release_derivation_exercises_weighted_and_hard_gates() -> None:
    manifest = copy.deepcopy(_manifest())
    manifest["mandatory_release_blockers"] = []
    for case in manifest["cases"]:
        for result in case["results"].values():
            result["status"] = "complete"
            result["metrics"] = {metric: 100 for metric in _METRICS}
            result["prohibited_claim_occurrences"] = 0
            result["cross_company_contamination_occurrences"] = 0

    assert _derive_release_verdict(manifest) == "APPROVED_FOR_SINGLE_USER_STAGE_B"
    manifest["cases"][0]["results"]["source_path"]["metrics"]["factual_support"] = 89
    assert _derive_release_verdict(manifest) == "REJECTED"


def test_verdict_document_is_unambiguous_and_matches_manifest() -> None:
    text = _VERDICT_PATH.read_text(encoding="utf-8")
    verdicts = [
        line.strip()
        for line in text.splitlines()
        if line.strip() in {"APPROVED_FOR_SINGLE_USER_STAGE_B", "REJECTED"}
    ]

    assert verdicts == [_manifest()["verdict"]]
    assert text.rstrip().endswith("REJECTED")


def test_universal_bundle_remains_local_only_and_task19_is_not_delivered() -> None:
    bundle = json.loads(
        (_ROOT / "integrations/n8n/stage-b-local-bundle.json").read_text(encoding="utf-8")
    )

    assert bundle["deployment_state"] == "LOCAL_MOCK_ONLY_NOT_CLOUD_VERIFIED"
    assert not (_ROOT / "ops/Containerfile").exists()
