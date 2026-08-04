import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from seo_orchestrator.canonical import canonical_json, sha256_fingerprint
from seo_orchestrator.db.connection import connect, transaction
from seo_orchestrator.db.migrations import migrate
from seo_orchestrator.db.repositories import SnapshotRepository
from seo_orchestrator.services.briefs import BriefService, UpdateBrief
from seo_orchestrator.services.company_cards import (
    AudienceData,
    CompanyCardService,
    CompanyProfileData,
    CreateAudience,
    CreateCompany,
    CreateDirection,
    DirectionData,
    ReviseCompany,
)
from seo_orchestrator.services.snapshots import SnapshotCompiler

NOW = datetime(2026, 8, 3, 15, tzinfo=UTC)
FIXTURE_ROOT = Path(__file__).parents[2] / "fixtures" / "companies"


def _fixture(name: str = "avtomalyar.json") -> dict[str, Any]:
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


def _seed_validated_brief(conn: Any) -> str:
    fixture = _fixture()
    cards = CompanyCardService(conn, clock=lambda: NOW)
    cards.create_company(
        CreateCompany(
            company_id=fixture["company_id"],
            company_profile_id=fixture["company_profile_id"],
            actor_id="invented-actor",
            replacement=CompanyProfileData.model_validate(fixture["profile"]),
        )
    )
    direction = fixture["direction"]
    cards.create_direction(
        CreateDirection(
            company_id=fixture["company_id"],
            company_profile_version=1,
            direction_id=direction["direction_id"],
            actor_id="invented-actor",
            replacement=DirectionData.model_validate(direction["data"]),
        )
    )
    audience = direction["audience"]
    cards.create_audience(
        CreateAudience(
            company_id=fixture["company_id"],
            direction_id=direction["direction_id"],
            direction_version=1,
            audience_segment_id=audience["audience_segment_id"],
            actor_id="invented-actor",
            replacement=AudienceData.model_validate(audience["data"]),
        )
    )
    briefs = BriefService(conn, clock=lambda: NOW, id_factory=lambda: "brief-snapshot-one")
    draft = briefs.start_brief("invented-actor", "avtomalyar")
    briefs.update_brief(
        UpdateBrief(
            brief_id=draft.brief_id,
            actor_id="invented-actor",
            company_id="avtomalyar",
            direction_id="car-painting",
            direction_version=1,
            audience_segment_id="private-car-owners",
            audience_version=1,
            page_type="service-page",
            goal="Explain invented refinishing services.",
            target_language="en",
            locale="en-US",
            page_structure=("Overview", "Process", "Estimate"),
            primary_keyword="invented refinishing service",
            keywords=("vehicle painting",),
            lsi_terms=("surface preparation",),
            competitor_urls=("https://example.test/reference",),
            current_page_context="Invented current-page context.",
        )
    )
    briefs.validate_brief("avtomalyar", draft.brief_id, "invented-actor")
    return draft.brief_id


def test_compile_snapshot_stores_canonical_exact_context_hash_and_is_idempotent(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "snapshots.db")
    try:
        migrate(conn)
        with transaction(conn):
            brief_id = _seed_validated_brief(conn)
            compiler = SnapshotCompiler(
                conn,
                company_id="avtomalyar",
                clock=lambda: NOW,
                id_factory=lambda: "snapshot-invented-one",
            )
            first = compiler.compile_snapshot(brief_id, 7)
            repeated = compiler.compile_snapshot(brief_id, 7)

        context = first.thawed_compiled_context()
        assert set(context) == {
            "schema_version",
            "company",
            "direction",
            "audience",
            "brief",
            "prompt_set_version",
        }
        assert context["schema_version"] == 1
        assert context["prompt_set_version"] == 7
        assert context["company"]["company_profile_version"] == 1
        assert context["direction"]["direction_version"] == 1
        assert context["audience"]["audience_version"] == 1
        assert first.snapshot_hash == sha256_fingerprint(context)
        assert repeated == first
        stored_bytes = bytes(
            conn.execute(
                "SELECT compiled_context FROM execution_snapshots WHERE snapshot_id = ?",
                (first.snapshot_id,),
            ).fetchone()[0]
        )
        assert stored_bytes == canonical_json(context)
        assert conn.execute("SELECT COUNT(*) FROM execution_snapshots").fetchone() == (1,)
    finally:
        conn.close()


def _seed_revised_validated_brief(conn: Any) -> str:
    fixture = _fixture()
    cards = CompanyCardService(conn, clock=lambda: NOW)
    revised_profile = CompanyProfileData.model_validate(
        {**fixture["profile"], "brand_summary": "Revised invented company profile."}
    )
    cards.revise_company(
        ReviseCompany(
            company_id="avtomalyar",
            actor_id="invented-actor",
            expected_current_version=1,
            replacement=revised_profile,
        )
    )
    direction = fixture["direction"]
    cards.create_direction(
        CreateDirection(
            company_id="avtomalyar",
            company_profile_version=2,
            direction_id="revised-painting",
            actor_id="invented-actor",
            replacement=DirectionData.model_validate(
                {**direction["data"], "name": "Revised Painting"}
            ),
        )
    )
    cards.create_audience(
        CreateAudience(
            company_id="avtomalyar",
            direction_id="revised-painting",
            direction_version=1,
            audience_segment_id="revised-owners",
            actor_id="invented-actor",
            replacement=AudienceData.model_validate(direction["audience"]["data"]),
        )
    )
    briefs = BriefService(conn, clock=lambda: NOW, id_factory=lambda: "brief-snapshot-two")
    draft = briefs.start_brief("invented-actor", "avtomalyar")
    briefs.update_brief(
        UpdateBrief(
            brief_id=draft.brief_id,
            actor_id="invented-actor",
            company_id="avtomalyar",
            direction_id="revised-painting",
            direction_version=1,
            audience_segment_id="revised-owners",
            audience_version=1,
            page_type="service-page",
            goal="Explain revised invented refinishing services.",
            target_language="en",
            locale="en-US",
            page_structure=("Revised overview", "Process"),
            primary_keyword="revised invented refinishing",
            keywords=("revised vehicle painting",),
            lsi_terms=("revised surface preparation",),
            competitor_urls=("https://example.test/revised-reference",),
            current_page_context="Revised invented current-page context.",
        )
    )
    briefs.validate_brief("avtomalyar", draft.brief_id, "invented-actor")
    return draft.brief_id


def test_snapshot_remains_immutable_after_revision_and_new_exact_versions_change_hash(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "snapshot-history.db")
    try:
        migrate(conn)
        with transaction(conn):
            first_brief_id = _seed_validated_brief(conn)
            first = SnapshotCompiler(
                conn,
                company_id="avtomalyar",
                clock=lambda: NOW,
                id_factory=lambda: "snapshot-history-one",
            ).compile_snapshot(first_brief_id, 7)
        original_bytes = bytes(
            conn.execute(
                "SELECT compiled_context FROM execution_snapshots WHERE snapshot_id = ?",
                (first.snapshot_id,),
            ).fetchone()[0]
        )

        with transaction(conn):
            BriefService(conn, clock=lambda: NOW).update_brief(
                UpdateBrief(
                    brief_id=first_brief_id,
                    actor_id="invented-actor",
                    company_id="avtomalyar",
                    goal="Legally revised goal after snapshot compilation.",
                )
            )

        repository = SnapshotRepository(conn)
        assert repository.get_snapshot("avtomalyar", first.snapshot_id) == first

        with transaction(conn):
            second_brief_id = _seed_revised_validated_brief(conn)
            second = SnapshotCompiler(
                conn,
                company_id="avtomalyar",
                clock=lambda: NOW,
                id_factory=lambda: "snapshot-history-two",
            ).compile_snapshot(second_brief_id, 7)

        historical = repository.get_snapshot("avtomalyar", first.snapshot_id)
        assert historical == first
        assert bytes(
            conn.execute(
                "SELECT compiled_context FROM execution_snapshots WHERE snapshot_id = ?",
                (first.snapshot_id,),
            ).fetchone()[0]
        ) == original_bytes
        assert historical.snapshot_hash == sha256_fingerprint(
            json.loads(original_bytes)
        )
        assert historical.thawed_compiled_context()["company"][
            "company_profile_version"
        ] == 1
        assert second.company_profile_version == 2
        assert second.thawed_compiled_context()["company"]["brand_summary"] == (
            "Revised invented company profile."
        )
        assert second.snapshot_hash != first.snapshot_hash
        with pytest.raises(LookupError, match="record not found"):
            repository.get_snapshot("sweet-world", first.snapshot_id)
    finally:
        conn.close()


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE execution_snapshots SET snapshot_hash = 'tampered'",
        "DELETE FROM execution_snapshots",
    ],
)
def test_execution_snapshots_are_append_only_at_storage_boundary(
    tmp_path: Path,
    statement: str,
) -> None:
    conn = connect(tmp_path / "snapshot-append-only.db")
    try:
        migrate(conn)
        with transaction(conn):
            brief_id = _seed_validated_brief(conn)
            snapshot = SnapshotCompiler(
                conn,
                company_id="avtomalyar",
                clock=lambda: NOW,
                id_factory=lambda: "snapshot-append-only",
            ).compile_snapshot(brief_id, 1)

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(statement)
        conn.rollback()

        assert SnapshotRepository(conn).get_snapshot(
            "avtomalyar", snapshot.snapshot_id
        ) == snapshot
    finally:
        conn.close()


def test_snapshot_compilation_is_company_scoped_and_missing_scope_is_non_oracular(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "snapshot-isolation.db")
    try:
        migrate(conn)
        with transaction(conn):
            brief_id = _seed_validated_brief(conn)

        for company_id in ("sweet-world", "missing-company"):
            with pytest.raises(LookupError, match="record not found"):
                SnapshotCompiler(conn, company_id=company_id).compile_snapshot(brief_id, 1)
        assert conn.execute("SELECT COUNT(*) FROM execution_snapshots").fetchone() == (0,)
    finally:
        conn.close()


def test_snapshot_compiler_leaves_transaction_ownership_and_rollback_to_caller(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "snapshot-rollback.db")
    try:
        migrate(conn)
        with transaction(conn):
            brief_id = _seed_validated_brief(conn)

        conn.execute("BEGIN IMMEDIATE")
        SnapshotCompiler(
            conn,
            company_id="avtomalyar",
            clock=lambda: NOW,
            id_factory=lambda: "snapshot-rolled-back",
        ).compile_snapshot(brief_id, 1)
        assert conn.in_transaction
        conn.rollback()
        assert conn.execute("SELECT COUNT(*) FROM execution_snapshots").fetchone() == (0,)
        assert SnapshotRepository(conn).get_snapshot_by_hash(
            "avtomalyar", "0" * 64
        ) is None
    finally:
        conn.close()
