class CanonicalizationError(TypeError):
    """Raised when a value cannot be represented by the canonical JSON contract."""


class NotFound(LookupError):
    """Raised when a record is absent from the caller's declared scope."""

    def __init__(self) -> None:
        super().__init__("record not found")


class VersionConflict(RuntimeError):
    """Raised when optimistic version preconditions are stale."""

    code = "VERSION_CONFLICT"

    def __init__(self) -> None:
        super().__init__("expected current version does not match")


class CompanyArchived(RuntimeError):
    """Raised when a write is attempted for an archived company."""

    code = "COMPANY_ARCHIVED"

    def __init__(self) -> None:
        super().__init__("company is archived")


class StateConflict(RuntimeError):
    """Raised when a job state compare-and-swap precondition is stale."""

    code = "STATE_CONFLICT"

    def __init__(self) -> None:
        super().__init__("expected job state does not match")


class InvalidTransition(RuntimeError):
    """Raised when an ordered state pair is absent from the frozen graph."""

    code = "INVALID_TRANSITION"

    def __init__(self) -> None:
        super().__init__("job state transition is not allowed")


class DataIntegrityError(RuntimeError):
    """Raised when persisted state fails canonical or provenance verification."""

    code = "DATA_INTEGRITY"

    def __init__(self) -> None:
        super().__init__("persisted data failed integrity verification")


class MigrationError(RuntimeError):
    """Raised when schema migration history is not a known contiguous prefix."""

    code = "MIGRATION_INVALID"

    def __init__(self) -> None:
        super().__init__("database migration history is invalid")


class ApprovalInvalid(RuntimeError):
    """Raised when submitted or durable approval fingerprints fail closed."""

    code = "APPROVAL_INVALID"

    def __init__(self) -> None:
        super().__init__("approval does not match the durable snapshot and plan")
