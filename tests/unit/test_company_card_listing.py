import json
from datetime import UTC, datetime
from pathlib import Path

from seo_orchestrator.db.connection import connect, transaction
from seo_orchestrator.db.migrations import migrate
from seo_orchestrator.services.company_cards import (
    CompanyCardService,
    CompanyProfileData,
    CreateCompany,
    ReviseCompany,
)

NOW = datetime(2026, 8, 5, tzinfo=UTC)
FIXTURE_ROOT = Path(__file__).parents[2] / "fixtures" / "companies"


def _fixture(name: str) -> dict[str, object]:
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


def _create_company(service: CompanyCardService, fixture: dict[str, object]) -> None:
    profile = fixture["profile"]
    assert isinstance(profile, dict)
    service.create_company(
        CreateCompany(
            company_id=str(fixture["company_id"]),
            company_profile_id=str(fixture["company_profile_id"]),
            actor_id="fixture-actor",
            replacement=CompanyProfileData.model_validate(profile),
        )
    )


def test_list_current_company_profiles_returns_active_current_versions_in_id_order(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "cards.db")
    try:
        migrate(conn)
        service = CompanyCardService(conn, clock=lambda: NOW)
        avtomalyar = _fixture("avtomalyar.json")
        sweet_world = _fixture("sweet-world.json")
        with transaction(conn):
            _create_company(service, sweet_world)
            _create_company(service, avtomalyar)
            original_profile = avtomalyar["profile"]
            assert isinstance(original_profile, dict)
            revised = service.revise_company(
                ReviseCompany(
                    company_id="avtomalyar",
                    actor_id="fixture-editor",
                    expected_current_version=1,
                    replacement=CompanyProfileData.model_validate(
                        {**original_profile, "name": "Avtomalyar Current"}
                    ),
                )
            )
            service.archive_company("sweet-world", "fixture-archivist")

        assert [profile.company_id for profile in service.list_current_company_profiles()] == [
            "avtomalyar"
        ]
        assert service.list_current_company_profiles() == (revised,)
    finally:
        conn.close()
