class StatsServiceError(Exception):
    pass


class InvalidStatsRequestError(StatsServiceError):
    pass


class StatsNotFoundError(StatsServiceError):
    pass
    