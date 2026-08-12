import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from seo_orchestrator.db.connection import connect, transaction
from seo_orchestrator.db.migrations import migrate
from seo_orchestrator.services.briefs import BriefService, UpdateBrief, ValidatedBrief
from seo_orchestrator.services.company_cards import (
    AudienceData,
    CompanyCardService,
    CompanyProfileData,
    CreateAudience,
    CreateCompany,
    CreateDirection,
    DirectionData,
    ReviseDirection,
)

NOW = datetime(2026, 8, 3, 12, tzinfo=UTC)
FIXTURE_ROOT = Path(__file__).parents[2] / "fixtures" / "companies"


def _fixture(name: str = "avtomalyar.json") -> dict[str, Any]:
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


def _seed_company(conn: Any, name: str = "avtomalyar.json") -> dict[str, Any]:
    fixture = _fixture(name)
    CompanyCardService(conn, clock=lambda: NOW).create_company(
        CreateCompany(
            company_id=fixture["company_id"],
            company_profile_id=fixture["company_profile_id"],
            actor_id="invented-actor",
            replacement=CompanyProfileData.model_validate(fixture["profile"]),
        )
    )
    return fixture


def _seed_card(conn: Any, name: str = "avtomalyar.json") -> dict[str, Any]:
    fixture = _seed_company(conn, name)
    direction = fixture["direction"]
    service = CompanyCardService(conn, clock=lambda: NOW)
    service.create_direction(
        CreateDirection(
            company_id=fixture["company_id"],
            company_profile_version=1,
            direction_id=direction["direction_id"],
            actor_id="invented-actor",
            replacement=DirectionData.model_validate(direction["data"]),
        )
    )
    service.create_direction(
        CreateDirection(
            company_id=fixture["company_id"],
            company_profile_version=1,
            direction_id="collision-repair",
            actor_id="invented-actor",
            replacement=DirectionData.model_validate(
                {**direction["data"], "name": "Collision Repair"}
            ),
        )
    )
    audience = direction["audience"]
    service.create_audience(
        CreateAudience(
            company_id=fixture["company_id"],
            direction_id=direction["direction_id"],
            direction_version=1,
            audience_segment_id=audience["audience_segment_id"],
            actor_id="invented-actor",
            replacement=AudienceData.model_validate(audience["data"]),
        )
    )
    return fixture


def test_start_brief_persists_resumable_company_version_without_committing(tmp_path: Path) -> None:
    conn = connect(tmp_path / "briefs.db")
    try:
        migrate(conn)
        with transaction(conn):
            _seed_company(conn)

        conn.execute("BEGIN IMMEDIATE")
        draft = BriefService(
            conn,
            clock=lambda: NOW,
            id_factory=lambda: "brief-invented-one",
        ).start_brief("invented-actor", "avtomalyar")

        assert draft.brief_id == "brief-invented-one"
        assert draft.company_id == "avtomalyar"
        assert draft.company_profile_version == 1
        assert draft.direction_id is None
        assert draft.created_by == "invented-actor"
        assert conn.in_transaction
        conn.rollback()
        assert conn.execute("SELECT COUNT(*) FROM brief_drafts").fetchone() == (0,)
    finally:
        conn.close()


def test_update_brief_resumes_saved_fields_and_direction_change_clears_derived_fields(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "briefs.db")
    try:
        migrate(conn)
        service = BriefService(conn, clock=lambda: NOW, id_factory=lambda: "brief-resume-one")
        with transaction(conn):
            _seed_card(conn)
            draft = service.start_brief("invented-actor", "avtomalyar")
            selected = service.update_brief(
                UpdateBrief(
                    brief_id=draft.brief_id,
                    actor_id="invented-actor",
                    company_id="avtomalyar",
                    direction_id="car-painting",
                    direction_version=1,
                    audience_segment_id="private-car-owners",
                    audience_version=1,
                    page_structure=("Inherited overview",),
                    category_context="Inherited vehicle category",
                )
            )

            changed = service.update_brief(
                UpdateBrief(
                    brief_id=draft.brief_id,
                    actor_id="invented-actor",
                    company_id="avtomalyar",
                    direction_id="collision-repair",
                    direction_version=1,
                )
            )

        assert selected.audience_segment_id == "private-car-owners"
        assert changed.audience_segment_id is None
        assert changed.audience_version is None
        assert changed.page_structure is None
        assert changed.category_context is None
        assert service.get_brief("avtomalyar", draft.brief_id, "invented-actor") == changed
    finally:
        conn.close()


def test_company_change_clears_all_child_and_inherited_values_before_other_updates(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "briefs.db")
    try:
        migrate(conn)
        service = BriefService(conn, clock=lambda: NOW, id_factory=lambda: "brief-company-change")
        with transaction(conn):
            _seed_card(conn)
            _seed_card(conn, "sweet-world.json")
            draft = service.start_brief("invented-actor", "avtomalyar")
            service.update_brief(
                UpdateBrief(
                    brief_id=draft.brief_id,
                    actor_id="invented-actor",
                    company_id="avtomalyar",
                    direction_id="car-painting",
                    direction_version=1,
                    audience_segment_id="private-car-owners",
                    audience_version=1,
                    page_structure=("Inherited overview",),
                    category_context="Inherited vehicle category",
                )
            )
            changed = service.update_brief(
                UpdateBrief(
                    brief_id=draft.brief_id,
                    actor_id="invented-actor",
                    company_id="avtomalyar",
                    replacement_company_id="sweet-world",
                    direction_id="car-painting",
                    direction_version=1,
                    audience_segment_id="private-car-owners",
                    audience_version=1,
                )
            )

        assert changed.company_id == "sweet-world"
        assert changed.company_profile_version == 1
        assert changed.direction_id is None
        assert changed.audience_segment_id is None
        assert changed.page_structure is None
        assert changed.category_context is None
        with pytest.raises(LookupError, match="record not found"):
            service.get_brief("avtomalyar", draft.brief_id, "invented-actor")
        assert service.get_brief("sweet-world", draft.brief_id, "invented-actor") == changed
    finally:
        conn.close()


def test_validate_brief_returns_exact_complete_domain_value_and_marks_saved_draft(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "briefs.db")
    try:
        migrate(conn)
        service = BriefService(conn, clock=lambda: NOW, id_factory=lambda: "brief-validate-one")
        with transaction(conn):
            _seed_card(conn)
            draft = service.start_brief("invented-actor", "avtomalyar")
            service.update_brief(
                UpdateBrief(
                    brief_id=draft.brief_id,
                    actor_id="invented-actor",
                    company_id="avtomalyar",
                    direction_id="car-painting",
                    direction_version=1,
                    audience_segment_id="private-car-owners",
                    audience_version=1,
                    page_type="service-page",
                    goal="Explain an invented vehicle refinishing service.",
                    target_language="en",
                    locale="en-US",
                    page_structure=("Overview", "Process", "Estimate"),
                    primary_keyword="invented car painting",
                    keywords=("vehicle refinishing",),
                    lsi_terms=("surface preparation",),
                    competitor_urls=("https://example.test/reference",),
                    current_page_context="Invented current-page source text.",
                )
            )
            validated = service.validate_brief(
                "avtomalyar", draft.brief_id, "invented-actor"
            )

        assert isinstance(validated, ValidatedBrief)
        assert validated.company_profile_version == 1
        assert validated.direction_version == 1
        assert validated.audience_version == 1
        assert validated.competitor_urls == ("https://example.test/reference",)
        assert conn.execute(
            "SELECT status FROM brief_drafts WHERE brief_id = ?", (draft.brief_id,)
        ).fetchone() == ("validated",)
    finally:
        conn.close()


def test_update_rejects_incomplete_version_pairs_without_changing_saved_draft(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "briefs.db")
    try:
        migrate(conn)
        service = BriefService(conn, clock=lambda: NOW, id_factory=lambda: "brief-version-pair")
        with transaction(conn):
            _seed_card(conn)
            draft = service.start_brief("invented-actor", "avtomalyar")
            with pytest.raises(ValueError, match="direction_id and direction_version"):
                service.update_brief(
                    UpdateBrief(
                        brief_id=draft.brief_id,
                        actor_id="invented-actor",
                        company_id="avtomalyar",
                        direction_id="car-painting",
                    )
                )

        assert service.get_brief("avtomalyar", draft.brief_id, "invented-actor") == draft
    finally:
        conn.close()


def test_update_rejects_incomplete_audience_pair_without_changing_saved_draft(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "briefs-audience-pair.db")
    try:
        migrate(conn)
        service = BriefService(conn, clock=lambda: NOW, id_factory=lambda: "brief-audience-pair")
        with transaction(conn):
            _seed_card(conn)
            draft = service.start_brief("invented-actor", "avtomalyar")
            with pytest.raises(ValueError, match="audience_segment_id and audience_version"):
                service.update_brief(
                    UpdateBrief(
                        brief_id=draft.brief_id,
                        actor_id="invented-actor",
                        company_id="avtomalyar",
                        audience_version=1,
                    )
                )

        assert service.get_brief("avtomalyar", draft.brief_id, "invented-actor") == draft
    finally:
        conn.close()


def test_update_rejects_cross_company_missing_and_cross_version_children_atomically(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "brief-owned-versions.db")
    try:
        migrate(conn)
        service = BriefService(conn, clock=lambda: NOW, id_factory=lambda: "brief-owned-versions")
        with transaction(conn):
            fixture = _seed_card(conn)
            _seed_card(conn, "sweet-world.json")
            cards = CompanyCardService(conn, clock=lambda: NOW)
            cards.revise_direction(
                ReviseDirection(
                    company_id="avtomalyar",
                    company_profile_version=1,
                    direction_id="car-painting",
                    actor_id="invented-actor",
                    expected_current_version=1,
                    replacement=DirectionData.model_validate(fixture["direction"]["data"]),
                )
            )
            draft = service.start_brief("invented-actor", "avtomalyar")

            invalid_updates = (
                UpdateBrief(
                    brief_id=draft.brief_id,
                    actor_id="invented-actor",
                    company_id="avtomalyar",
                    direction_id="wedding-cakes",
                    direction_version=1,
                ),
                UpdateBrief(
                    brief_id=draft.brief_id,
                    actor_id="invented-actor",
                    company_id="avtomalyar",
                    direction_id="car-painting",
                    direction_version=99,
                ),
                UpdateBrief(
                    brief_id=draft.brief_id,
                    actor_id="invented-actor",
                    company_id="avtomalyar",
                    direction_id="car-painting",
                    direction_version=2,
                    audience_segment_id="private-car-owners",
                    audience_version=1,
                ),
            )
            for command in invalid_updates:
                with pytest.raises(LookupError, match="record not found"):
                    service.update_brief(command)
                assert service.get_brief(
                    "avtomalyar", draft.brief_id, "invented-actor"
                ) == draft
    finally:
        conn.close()


def test_brief_scope_is_non_oracular_and_validation_uses_explicit_company(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "brief-isolation.db")
    try:
        migrate(conn)
        service = BriefService(conn, clock=lambda: NOW, id_factory=lambda: "brief-isolated")
        with transaction(conn):
            _seed_card(conn)
            _seed_card(conn, "sweet-world.json")
            draft = service.start_brief("invented-actor", "avtomalyar")

        for company_id, actor_id in (
            ("sweet-world", "invented-actor"),
            ("avtomalyar", "different-actor"),
            ("sweet-world", "different-actor"),
        ):
            with pytest.raises(LookupError, match="record not found"):
                service.get_brief(company_id, draft.brief_id, actor_id)
            with pytest.raises(LookupError, match="record not found"):
                service.validate_brief(company_id, draft.brief_id, actor_id)

        with pytest.raises(LookupError, match="record not found"):
            service.update_brief(
                UpdateBrief(
                    brief_id=draft.brief_id,
                    actor_id="different-actor",
                    company_id="avtomalyar",
                    direction_id="car-painting",
                )
            )
    finally:
        conn.close()


def test_validate_incomplete_brief_is_side_effect_free_and_caller_can_rollback_success(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "brief-validation-rollback.db")
    try:
        migrate(conn)
        service = BriefService(conn, clock=lambda: NOW, id_factory=lambda: "brief-validation-rollback")
        with transaction(conn):
            _seed_card(conn)
            draft = service.start_brief("invented-actor", "avtomalyar")

        with pytest.raises(ValueError, match="brief is incomplete"):
            service.validate_brief("avtomalyar", draft.brief_id, "invented-actor")
        assert conn.execute(
            "SELECT status FROM brief_drafts WHERE brief_id = ?", (draft.brief_id,)
        ).fetchone() == ("draft",)

        conn.execute("BEGIN IMMEDIATE")
        service.update_brief(
            UpdateBrief(
                brief_id=draft.brief_id,
                actor_id="invented-actor",
                company_id="avtomalyar",
                direction_id="car-painting",
                direction_version=1,
                audience_segment_id="private-car-owners",
                audience_version=1,
                page_type="service-page",
                goal="Explain an invented service.",
                target_language="en",
                locale="en-US",
                page_structure=("Overview",),
                primary_keyword="invented service",
                keywords=("invented keyword",),
                lsi_terms=("invented term",),
                competitor_urls=("https://example.test/reference",),
                current_page_context="Invented source text.",
            )
        )
        service.validate_brief("avtomalyar", draft.brief_id, "invented-actor")
        assert conn.in_transaction
        conn.rollback()
        assert conn.execute(
            "SELECT status FROM brief_drafts WHERE brief_id = ?", (draft.brief_id,)
        ).fetchone() == ("draft",)
    finally:
        conn.close()
