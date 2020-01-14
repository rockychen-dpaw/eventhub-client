import socket
import logging

from playhouse import reflection

from eventhub_utils.settings import *
from eventhub_utils.database import (PostgresqlExtDatabase,PooledPostgresqlExtDatabase)
from eventhub_utils import parse_db_connection_string,classproperty

logging.getLogger("pubsub").setLevel(logging.DEBUG)

HOSTNAME = socket.gethostname()

class DatabaseConfig(object):
    default = parse_db_connection_string(env("EVENTHUB_DATABASE_URL",vtype=str,required=True))

class Database(object):
    class Default(object):
        _databases = {}
        @classmethod
        def get(cls,name="default",thread_safe=True):
            if name not in cls._databases:
                cls._databases[name] = PostgresqlExtDatabase(DatabaseConfig.default["dbname"], 
                    user=DatabaseConfig.default["user"], 
                    password=DatabaseConfig.default["password"],
                    host=DatabaseConfig.default["host"], 
                    port=DatabaseConfig.default["port"],
                    thread_safe=thread_safe
                ) 
            return cls._databases[name]

class DatabasePool(object):
    default = PooledPostgresqlExtDatabase(DatabaseConfig.default["dbname"], 
        user=DatabaseConfig.default["user"], 
        password=DatabaseConfig.default["password"],
        host=DatabaseConfig.default["host"], 
        port=DatabaseConfig.default["port"],
        max_connections=3,
        stale_timeout=300,
        timeout=5
    ) 

class Introspector(object):
    default = reflection.Introspector.from_database(DatabasePool.default,schema="public")
