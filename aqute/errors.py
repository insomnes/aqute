class AquteError(Exception):
    """Base for aqute related errors"""


class AquteTaskTimeoutError(AquteError):
    """Raised when Aqute task coroutine times out if timeout is set"""


class AquteTooManyTasksFailedError(AquteError):
    """Raised when too many tasks failed"""
