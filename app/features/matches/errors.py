from __future__ import annotations


class NotFoundError(Exception):
    pass


class MatchServiceError(Exception):
    pass


class InvalidIDError(MatchServiceError):
    pass


class ApprovingError(MatchServiceError):
    """An approve holds the match, so it cannot change now."""


class ParseError(MatchServiceError):
    pass


__all__ = [
    "ApprovingError",
    "InvalidIDError",
    "MatchServiceError",
    "NotFoundError",
    "ParseError",
]
