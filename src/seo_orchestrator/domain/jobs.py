"""Durable SEO job states and their exact transition graph."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class JobState(StrEnum):
    DRAFT = "DRAFT"
    VALIDATED = "VALIDATED"
    PLANNED = "PLANNED"
    AWAITING_PAID_APPROVAL = "AWAITING_PAID_APPROVAL"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    FAILED_FINAL = "FAILED_FINAL"
    CANCELED = "CANCELED"
    SUCCEEDED = "SUCCEEDED"
    AWAITING_EXPORT_APPROVAL = "AWAITING_EXPORT_APPROVAL"
    EXPORTED = "EXPORTED"


@dataclass(frozen=True, slots=True)
class SeoJob:
    job_id: str
    brief_id: str
    brief_fingerprint: str
    snapshot_id: str
    snapshot_hash: str
    company_id: str
    direction_id: str
    audience_segment_id: str
    state: JobState
    current_stage: str | None
    approved_plan_fingerprint: str | None
    approval_record_id: str | None
    attempt: int
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    error_code: str | None
    error_summary: str | None
    artifact_manifest_path: str | None
    company_profile_version: int | None = None
    direction_version: int | None = None
    audience_version: int | None = None
    prompt_set_version: int | None = None


ALLOWED_TRANSITIONS: frozenset[tuple[JobState, JobState]] = frozenset(
    {
        (JobState.DRAFT, JobState.VALIDATED),
        (JobState.VALIDATED, JobState.PLANNED),
        (JobState.PLANNED, JobState.AWAITING_PAID_APPROVAL),
        (JobState.AWAITING_PAID_APPROVAL, JobState.QUEUED),
        (JobState.QUEUED, JobState.RUNNING),
        (JobState.RUNNING, JobState.SUCCEEDED),
        (JobState.SUCCEEDED, JobState.AWAITING_EXPORT_APPROVAL),
        (JobState.AWAITING_EXPORT_APPROVAL, JobState.EXPORTED),
        (JobState.QUEUED, JobState.FAILED_RETRYABLE),
        (JobState.RUNNING, JobState.FAILED_RETRYABLE),
        (JobState.FAILED_RETRYABLE, JobState.QUEUED),
        (JobState.QUEUED, JobState.CANCELED),
        (JobState.RUNNING, JobState.CANCELED),
        (JobState.RUNNING, JobState.FAILED_FINAL),
    }
)


def is_transition_allowed(source: JobState, target: JobState) -> bool:
    """Return whether the ordered state pair is one of the 14 frozen graph edges."""
    return (source, target) in ALLOWED_TRANSITIONS
