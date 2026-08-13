from __future__ import annotations


class NotFoundError(Exception):
    pass


class MatchServiceError(Exception):
    pass


class InvalidIDError(MatchServiceError):
    pass


class ParseError(MatchServiceError):
    pass


__all__ = ["InvalidIDError", "MatchServiceError", "NotFoundError", "ParseError"]
