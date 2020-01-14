import pytz
import logging
from datetime import datetime

from .env import env

TIME_ZONE = env("TIME_ZONE",'Australia/Perth')
TZ = datetime.now(tz=pytz.timezone(TIME_ZONE)).tzinfo

logging.basicConfig(level="WARNING")

