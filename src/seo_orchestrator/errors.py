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
