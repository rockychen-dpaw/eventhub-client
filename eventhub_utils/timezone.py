from datetime import datetime

from . import settings


def now():
    """
    Return the current time with configured timezone
    """
    return datetime.now(tz=settings.TZ)

