import peewee
import playhouse.pool
import playhouse.postgres_ext

class ActiveContext(object):
    def __init__(self,database):
        self.database = database

        
    def __enter__(self):
        self.database.active_connect()
        self.database.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.database.__exit__(exc_type,exc_val,exc_tb)

class IsActiveMixin(object):
    @property
    def is_active(self):
        try:
            self.check_active()
            return True
        except:
            return False

    def check_active(self):
        if self.is_closed():
            raise peewee.ProgrammingError("Database is closed")
        self.execute_sql("SELECT 1;")

    def active_context(self):
        return ActiveContext(self)

class PostgresqlExtDatabase(IsActiveMixin,playhouse.postgres_ext.PostgresqlExtDatabase):
    def clean_if_inactive(self):
        if self.is_active:
            return

        if not self.is_closed():
            try:
                self.close()
            except:
                pass

    def active_connect(self):
        return self.connect(reuse_if_open=True,check_active=True)

    def connect(self,reuse_if_open=False,check_active=False):
        if not check_active:
            #use the original logic to get connection
            return super().connect(reuse_if_open)
        else:
            closed =  self.is_closed()
            result = super().connect(reuse_if_open)
            try:
                self.check_active()
                return result
            except:
                #database is not active
                if not closed and reuse_if_open:
                    #the connection is reused, reconnect again
                    #close the database first
                    try:
                        self.close()
                    except:
                        pass
                    #reconnect the database
                    result = super().connect()
                    #check whether connection is active or not,if inactive, throw exception directly
                    self.check_active()
                    return result
                else:
                    #the connection is newly created, throw the exception
                    raise


class PooledPostgresqlExtDatabase(IsActiveMixin,playhouse.pool.PooledPostgresqlExtDatabase):
    def clean_if_inactive(self):
        """
        Return True if cleaned; else return False
        """
        if self.is_active:
            return False
        try:
            self.manual_close()
        except:
            pass
        try:
            self.close_idle()
        except:
            pass
        return True

    def active_connect(self):
        return self.connect(reuse_if_open=True,check_active=True)

    def connect(self,reuse_if_open=False,check_active=False):
        result = super().connect(reuse_if_open=reuse_if_open)
        if not check_active:
            return result
        else:
            if self.clean_if_inactive():
                #connection is inactive,reget again.
                result = super().connect()
                #check connection again, if failed, database is not running or have connection issue.
                self.check_active()
                return result
            else:
                #connection is active
                return result

