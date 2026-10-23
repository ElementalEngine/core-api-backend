from __future__ import annotations


class NotFoundError(Exception):
    pass


class MatchServiceError(Exception):
    pass


class InvalidIDError(MatchServiceError):
    pass


class ApprovingError(MatchServiceError):
    """An approve holds the match, so it cannot change now."""


class RetryLaterError(MatchServiceError):
    """A write that clashed with another one; the same request may be sent again."""


class ParseError(MatchServiceError):
    pass


__all__ = [
    "ApprovingError",
    "InvalidIDError",
    "MatchServiceError",
    "NotFoundError",
    "ParseError",
    "RetryLaterError",
]
