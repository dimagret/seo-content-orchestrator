from datetime import UTC, datetime
from pathlib import Path

import pytest

from seo_orchestrator.db.connection import connect, transaction
from seo_orchestrator.db.migrations import migrate
from seo_orchestrator.errors import CompanyArchived, VersionConflict
from seo_orchestrator.services.company_cards import (
    AudienceData,
    CompanyCardService,
    CompanyProfileData,
    CreateAudience,
    CreateCompany,
    CreateDirection,
    DirectionData,
    ReviseAudience,
    ReviseCompany,
    ReviseDirection,
)

NOW = datetime(2026, 8, 3, tzinfo=UTC)
LATER = datetime(2026, 8, 4, tzinfo=UTC)


def _profile_data(*, name: str = "Avtomalyar Studio") -> CompanyProfileData:
    return CompanyProfileData(
        name=name,
        brand_summary="Invented vehicle refinishing studio.",
        products_services_overview="Invented paint consultation and refinishing services.",
        commercial_model="Direct service by appointment.",
        pricing_overview="Written estimates after inspection.",
        service_geography="Invented local service area.",
        value_propositions=("Documented preparation steps",),
        proof_points=("Invented sample portfolio",),
        certifications=("Invented technical training",),
        case_references=("Invented compact-car color matching case",),
        tools_and_process=("Color assessment", "Surface preparation"),
        tone_of_voice="Practical, calm, and specific.",
        positive_voice_examples=("We explain each preparation step.",),
        negative_voice_examples=("A perfect result is guaranteed.",),
        reading_level="General audience.",
        allowed_claims=("Written scope before work begins",),
        forbidden_claims=("Guaranteed flawless result",),
        compliance_requirements=("Mark all examples as invented",),
        default_language="en",
        default_locale="en-US",
    )


def test_create_company_returns_version_one_without_owning_commit(tmp_path: Path) -> None:
    conn = connect(tmp_path / "cards.db")
    try:
        migrate(conn)
        command = CreateCompany(
            company_id="avtomalyar",
            company_profile_id="avtomalyar-profile",
            actor_id="invented-actor",
            replacement=_profile_data(),
        )

        with transaction(conn):
            profile = CompanyCardService(conn, clock=lambda: NOW).create_company(command)
            assert conn.in_transaction

        assert profile.company_profile_version == 1
        assert profile.company_id == "avtomalyar"
        assert profile.company_profile_id == "avtomalyar-profile"
        assert profile.created_at == NOW
        assert profile.updated_at == NOW
    finally:
        conn.close()


def test_revise_company_appends_next_version_and_preserves_prior_row(tmp_path: Path) -> None:
    conn = connect(tmp_path / "cards.db")
    try:
        migrate(conn)
        times = iter((NOW, LATER))
        service = CompanyCardService(conn, clock=lambda: next(times))
        with transaction(conn):
            original = service.create_company(
                CreateCompany(
                    company_id="avtomalyar",
                    company_profile_id="avtomalyar-profile",
                    actor_id="invented-actor",
                    replacement=_profile_data(),
                )
            )
            revised = service.revise_company(
                ReviseCompany(
                    company_id="avtomalyar",
                    actor_id="invented-editor",
                    expected_current_version=1,
                    replacement=_profile_data(name="Avtomalyar Workshop"),
                )
            )

        assert revised.company_profile_version == 2
        assert revised.company_profile_id == original.company_profile_id
        assert revised.created_at == original.created_at
        assert revised.updated_at == LATER
        assert service.get_company_profile("avtomalyar", 1) == original
        assert service.get_company_profile("avtomalyar", 2) == revised
    finally:
        conn.close()


def test_revise_company_rejects_stale_expected_version(tmp_path: Path) -> None:
    conn = connect(tmp_path / "cards.db")
    try:
        migrate(conn)
        service = CompanyCardService(conn, clock=lambda: NOW)
        with transaction(conn):
            service.create_company(
                CreateCompany(
                    company_id="avtomalyar",
                    company_profile_id="avtomalyar-profile",
                    actor_id="invented-actor",
                    replacement=_profile_data(),
                )
            )
            with pytest.raises(VersionConflict) as caught:
                service.revise_company(
                    ReviseCompany(
                        company_id="avtomalyar",
                        actor_id="invented-editor",
                        expected_current_version=2,
                        replacement=_profile_data(name="Stale replacement"),
                    )
                )

        assert caught.value.code == "VERSION_CONFLICT"
        assert conn.execute(
            "SELECT COUNT(*) FROM company_profile_versions WHERE company_id = ?",
            ("avtomalyar",),
        ).fetchone() == (1,)
    finally:
        conn.close()


def _direction_data(*, name: str = "Car Painting") -> DirectionData:
    return DirectionData(
        name=name,
        offerings=("Invented refinishing consultation",),
        category_context="Invented vehicle paint service category.",
        prices_and_tariffs="Written estimates after inspection.",
        direction_value_propositions=("Preparation steps are documented",),
        direction_proof_points=("Invented finish sample",),
        direction_cases=("Invented hatchback panel example",),
        internal_link_catalog=("/invented-car-painting",),
        default_page_structure=("Service overview", "Preparation", "Estimate"),
        default_language="en",
        default_locale="en-US",
        allowed_claims=("A written scope is provided",),
        forbidden_claims=("Factory-perfect match guaranteed",),
    )


def test_create_direction_binds_exact_company_profile_version(tmp_path: Path) -> None:
    conn = connect(tmp_path / "cards.db")
    try:
        migrate(conn)
        service = CompanyCardService(conn, clock=lambda: NOW)
        with transaction(conn):
            service.create_company(
                CreateCompany(
                    company_id="avtomalyar",
                    company_profile_id="avtomalyar-profile",
                    actor_id="invented-actor",
                    replacement=_profile_data(),
                )
            )
            direction = service.create_direction(
                CreateDirection(
                    company_id="avtomalyar",
                    company_profile_version=1,
                    direction_id="car-painting",
                    actor_id="invented-actor",
                    replacement=_direction_data(),
                )
            )

        assert direction.direction_version == 1
        assert direction.company_profile_version == 1
        assert service.get_direction("avtomalyar", "car-painting", 1) == direction
    finally:
        conn.close()


def test_revise_direction_appends_next_version_and_preserves_prior_row(tmp_path: Path) -> None:
    conn = connect(tmp_path / "cards.db")
    try:
        migrate(conn)
        times = iter((NOW, NOW, LATER))
        service = CompanyCardService(conn, clock=lambda: next(times))
        with transaction(conn):
            service.create_company(
                CreateCompany(
                    company_id="avtomalyar",
                    company_profile_id="avtomalyar-profile",
                    actor_id="invented-actor",
                    replacement=_profile_data(),
                )
            )
            original = service.create_direction(
                CreateDirection(
                    company_id="avtomalyar",
                    company_profile_version=1,
                    direction_id="car-painting",
                    actor_id="invented-actor",
                    replacement=_direction_data(),
                )
            )
            revised = service.revise_direction(
                ReviseDirection(
                    company_id="avtomalyar",
                    company_profile_version=1,
                    direction_id="car-painting",
                    actor_id="invented-editor",
                    expected_current_version=1,
                    replacement=_direction_data(name="Vehicle Paint Refinishing"),
                )
            )

        assert revised.direction_version == 2
        assert revised.created_at == original.created_at
        assert revised.updated_at == LATER
        assert service.get_direction("avtomalyar", "car-painting", 1) == original
        assert service.get_direction("avtomalyar", "car-painting", 2) == revised
    finally:
        conn.close()


def _audience_data() -> AudienceData:
    return AudienceData(
        name="Private Car Owners",
        buyer_roles=("Vehicle owner",),
        industry="Consumer vehicle care",
        company_or_customer_size="Individual customer",
        geography="Invented local service area",
        jobs_to_be_done=("Restore a vehicle panel finish",),
        pains_and_risks=("Unclear preparation quality",),
        objections=("Estimate uncertainty",),
        objection_responses=("Explain inspection and written scope",),
        selection_criteria=("Documented preparation process",),
        minimum_expectations=("Clear estimate",),
        purchase_triggers=("Visible paint damage",),
        budget_range="Estimate after inspection",
        decision_cycle="Several days",
        decision_participants=("Vehicle owner",),
        preferred_content_formats=("Service guide",),
    )


def test_create_audience_binds_exact_direction_version(tmp_path: Path) -> None:
    conn = connect(tmp_path / "cards.db")
    try:
        migrate(conn)
        service = CompanyCardService(conn, clock=lambda: NOW)
        with transaction(conn):
            service.create_company(
                CreateCompany(
                    company_id="avtomalyar",
                    company_profile_id="avtomalyar-profile",
                    actor_id="invented-actor",
                    replacement=_profile_data(),
                )
            )
            service.create_direction(
                CreateDirection(
                    company_id="avtomalyar",
                    company_profile_version=1,
                    direction_id="car-painting",
                    actor_id="invented-actor",
                    replacement=_direction_data(),
                )
            )
            audience = service.create_audience(
                CreateAudience(
                    company_id="avtomalyar",
                    direction_id="car-painting",
                    direction_version=1,
                    audience_segment_id="private-car-owners",
                    actor_id="invented-actor",
                    replacement=_audience_data(),
                )
            )

        assert audience.audience_version == 1
        assert audience.direction_version == 1
        assert service.get_audience(
            "avtomalyar", "car-painting", "private-car-owners", 1
        ) == audience
    finally:
        conn.close()


def test_revise_audience_appends_version_bound_to_revised_direction(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path / "cards.db")
    try:
        migrate(conn)
        times = iter((NOW, NOW, NOW, LATER, LATER))
        service = CompanyCardService(conn, clock=lambda: next(times))
        with transaction(conn):
            service.create_company(
                CreateCompany(
                    company_id="avtomalyar",
                    company_profile_id="avtomalyar-profile",
                    actor_id="invented-actor",
                    replacement=_profile_data(),
                )
            )
            service.create_direction(
                CreateDirection(
                    company_id="avtomalyar",
                    company_profile_version=1,
                    direction_id="car-painting",
                    actor_id="invented-actor",
                    replacement=_direction_data(),
                )
            )
            original = service.create_audience(
                CreateAudience(
                    company_id="avtomalyar",
                    direction_id="car-painting",
                    direction_version=1,
                    audience_segment_id="private-car-owners",
                    actor_id="invented-actor",
                    replacement=_audience_data(),
                )
            )
            service.revise_direction(
                ReviseDirection(
                    company_id="avtomalyar",
                    company_profile_version=1,
                    direction_id="car-painting",
                    actor_id="invented-editor",
                    expected_current_version=1,
                    replacement=_direction_data(name="Vehicle Paint Refinishing"),
                )
            )
            revised = service.revise_audience(
                ReviseAudience(
                    company_id="avtomalyar",
                    direction_id="car-painting",
                    direction_version=2,
                    audience_segment_id="private-car-owners",
                    actor_id="invented-editor",
                    expected_current_version=1,
                    replacement=_audience_data(),
                )
            )
            with pytest.raises(VersionConflict):
                service.revise_audience(
                    ReviseAudience(
                        company_id="avtomalyar",
                        direction_id="car-painting",
                        direction_version=2,
                        audience_segment_id="private-car-owners",
                        actor_id="invented-stale-editor",
                        expected_current_version=1,
                        replacement=_audience_data(),
                    )
                )

        assert revised.audience_version == 2
        assert revised.direction_version == 2
        assert revised.created_at == original.created_at
        assert revised.updated_at == LATER
        assert service.get_audience(
            "avtomalyar", "car-painting", "private-car-owners", 1
        ) == original
        assert service.get_audience(
            "avtomalyar", "car-painting", "private-car-owners", 2
        ) == revised
        assert conn.execute(
            """SELECT COUNT(*) FROM audience_segment_versions
               WHERE company_id = ? AND direction_id = ? AND audience_segment_id = ?""",
            ("avtomalyar", "car-painting", "private-car-owners"),
        ).fetchone() == (2,)
    finally:
        conn.close()


def test_archive_keeps_history_readable_but_blocks_new_card_drafts(tmp_path: Path) -> None:
    conn = connect(tmp_path / "cards.db")
    try:
        migrate(conn)
        service = CompanyCardService(conn, clock=lambda: NOW)
        with transaction(conn):
            profile = service.create_company(
                CreateCompany(
                    company_id="avtomalyar",
                    company_profile_id="avtomalyar-profile",
                    actor_id="invented-actor",
                    replacement=_profile_data(),
                )
            )
            service.archive_company("avtomalyar", "invented-archivist")
            with pytest.raises(CompanyArchived) as caught:
                service.create_direction(
                    CreateDirection(
                        company_id="avtomalyar",
                        company_profile_version=1,
                        direction_id="car-painting",
                        actor_id="invented-actor",
                        replacement=_direction_data(),
                    )
                )

        assert caught.value.code == "COMPANY_ARCHIVED"
        assert service.get_company_profile("avtomalyar", 1) == profile
    finally:
        conn.close()


def test_archive_blocks_company_revision(tmp_path: Path) -> None:
    conn = connect(tmp_path / "cards.db")
    try:
        migrate(conn)
        service = CompanyCardService(conn, clock=lambda: NOW)
        with transaction(conn):
            service.create_company(
                CreateCompany(
                    company_id="avtomalyar",
                    company_profile_id="avtomalyar-profile",
                    actor_id="invented-actor",
                    replacement=_profile_data(),
                )
            )
            service.archive_company("avtomalyar", "invented-archivist")
            with pytest.raises(CompanyArchived):
                service.revise_company(
                    ReviseCompany(
                        company_id="avtomalyar",
                        actor_id="invented-editor",
                        expected_current_version=1,
                        replacement=_profile_data(name="Forbidden revision"),
                    )
                )

        assert conn.execute(
            "SELECT COUNT(*) FROM company_profile_versions WHERE company_id = ?",
            ("avtomalyar",),
        ).fetchone() == (1,)
    finally:
        conn.close()


def test_archive_blocks_direction_revision(tmp_path: Path) -> None:
    conn = connect(tmp_path / "cards.db")
    try:
        migrate(conn)
        service = CompanyCardService(conn, clock=lambda: NOW)
        with transaction(conn):
            service.create_company(
                CreateCompany(
                    company_id="avtomalyar",
                    company_profile_id="avtomalyar-profile",
                    actor_id="invented-actor",
                    replacement=_profile_data(),
                )
            )
            service.create_direction(
                CreateDirection(
                    company_id="avtomalyar",
                    company_profile_version=1,
                    direction_id="car-painting",
                    actor_id="invented-actor",
                    replacement=_direction_data(),
                )
            )
            service.archive_company("avtomalyar", "invented-archivist")
            with pytest.raises(CompanyArchived):
                service.revise_direction(
                    ReviseDirection(
                        company_id="avtomalyar",
                        company_profile_version=1,
                        direction_id="car-painting",
                        actor_id="invented-editor",
                        expected_current_version=1,
                        replacement=_direction_data(name="Forbidden revision"),
                    )
                )

        assert conn.execute(
            """SELECT COUNT(*) FROM business_direction_versions
               WHERE company_id = ? AND direction_id = ?""",
            ("avtomalyar", "car-painting"),
        ).fetchone() == (1,)
    finally:
        conn.close()


def test_archive_blocks_audience_creation(tmp_path: Path) -> None:
    conn = connect(tmp_path / "cards.db")
    try:
        migrate(conn)
        service = CompanyCardService(conn, clock=lambda: NOW)
        with transaction(conn):
            service.create_company(
                CreateCompany(
                    company_id="avtomalyar",
                    company_profile_id="avtomalyar-profile",
                    actor_id="invented-actor",
                    replacement=_profile_data(),
                )
            )
            service.create_direction(
                CreateDirection(
                    company_id="avtomalyar",
                    company_profile_version=1,
                    direction_id="car-painting",
                    actor_id="invented-actor",
                    replacement=_direction_data(),
                )
            )
            service.archive_company("avtomalyar", "invented-archivist")
            with pytest.raises(CompanyArchived):
                service.create_audience(
                    CreateAudience(
                        company_id="avtomalyar",
                        direction_id="car-painting",
                        direction_version=1,
                        audience_segment_id="private-car-owners",
                        actor_id="invented-actor",
                        replacement=_audience_data(),
                    )
                )

        assert conn.execute(
            "SELECT COUNT(*) FROM audience_segment_versions WHERE company_id = ?",
            ("avtomalyar",),
        ).fetchone() == (0,)
    finally:
        conn.close()
