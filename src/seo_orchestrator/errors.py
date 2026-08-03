class CanonicalizationError(TypeError):
    """Raised when a value cannot be represented by the canonical JSON contract."""


class NotFound(LookupError):
    """Raised when a record is absent from the caller's declared scope."""

    def __init__(self) -> None:
        super().__init__("record not found")
