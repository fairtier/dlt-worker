class Recipient:
    @staticmethod
    def from_str(s: str) -> Recipient: ...
    # Not the inherited default (PYI029): pyrage implements this, and
    # `str(...)` is the only way to get the key string back out.
    def __str__(self) -> str: ...  # noqa: PYI029

class Identity:
    @staticmethod
    def generate() -> Identity: ...
    @staticmethod
    def from_str(s: str) -> Identity: ...
    def to_public(self) -> Recipient: ...
    # Not the inherited default (PYI029): pyrage implements this, and
    # `str(...)` is the only way to get the key string back out.
    def __str__(self) -> str: ...  # noqa: PYI029
