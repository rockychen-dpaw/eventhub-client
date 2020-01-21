from datetime import datetime
import pytz
import json
import re
import hashlib
import traceback

from . import decorators
from . import database

from . import settings
from . import timezone
from .env import env

from .classproperty import classproperty,cachedclassproperty


def print_callstack():
    for line in traceback.format_stack()[:-2]:
        print(line.strip())

def hashvalue(value):
    m = hashlib.sha1()
    m.update(value.encode('utf-8'))
    return m.hexdigest()

class JSONEncoder(json.JSONEncoder):
    """
    A JSON encoder to support encode datetime
    """
    def default(self,obj):
        if isinstance(obj,datetime):
            return {
                "_type":"datetime",
                "value":obj.astimezone(tz=settings.TZ).strftime("%Y-%m-%d %H:%M:%S.%f")
            }
        return json.JSONEncoder.default(self,obj)

class JSONDecoder(json.JSONDecoder):
    """
    A JSON decoder to support decode datetime
    """
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        if '_type' not in obj:
            return obj
        type = obj['_type']
        if type == 'datetime':
            return datetime.strptime(obj["value"],"%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=settings.TZ)
        else:
            return obj

db_connection_string_re = re.compile('^\s*(?P<database>(postgis)|(postgres))://(?P<user>[^@:]+)(:(?P<password>[0-9a-zA-Z]+))?@(?P<host>[^:\/\s]+)(:(?P<port>[1-9][0-9]*))?/(?P<dbname>[0-9a-zA-Z\-_]+)?\s*$')
def parse_db_connection_string(connection_string):
    """
    postgis://rockyc@localhost/bfrs
    """
    m = db_connection_string_re.match(connection_string)
    if not m:
        raise Exceptino("Invalid database configuration({})".format(connection_string))

    database_config = {
        "database":m.group("database"),
        "user":m.group("user"),
        "host":m.group("host"),
        "dbname":m.group("dbname"),
        "port" : int(m.group('port')) if m.group("port") else None,
        "password" : m.group('password') if m.group("password") else None
    }

    return database_config

