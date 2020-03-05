from datetime import timedelta
import imp

import peewee as models
from playhouse.postgres_ext import JSONField

from eventhub_utils import timezone,cachedclassproperty,classproperty,hashvalue

from . import settings

PROGRAMMATIC = 1
MANAGED = 2
SYSTEM = 999
TESTING = -1
UNITESTING = -2
    
CATEGORY_CHOICES = (
    (PROGRAMMATIC,"Programmatic"),
    (MANAGED,"Managed"),
    (SYSTEM,"System"),
    (TESTING,"Testing"),
    (UNITESTING,"Unitesting")
)

class BaseModel(models.Model):
    @classproperty
    def table_name(cls):
        return cls._meta.table_name

    @classproperty
    def database(cls):
        return cls._meta.database

    @classmethod
    def database_is_broken(cls):
        try:
            cls.database.execute_sql("select 1")
            return True
        except:
            return False

    class Meta:
        database = settings.DatabasePool.default 
        legacy_table_names = False

class User(BaseModel):
    username = models.CharField(max_length=128)
    first_name = models.CharField( max_length=30, )
    last_name = models.CharField(max_length=150)
    email = models.CharField(max_length=128)
    is_staff = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)
    date_joined = models.DateTimeField(default=timezone.now)
    password = models.CharField(max_length=128)
    last_login = models.DateTimeField(null=True)
    is_superuser = models.BooleanField(default=False)
    

    class Meta:
        table_name = 'auth_user'

try:
    User.PROGRAMMATIC,created = User.get_or_create(username="Programattic",defaults={
        'password':'',
        'is_superuser':False,
        'is_staff':False,
        'first_name':'Programmatic',
        'last_name':'',
        'email':'',
    })
except:
    pass


class AuditModel(BaseModel):
    creator = models.ForeignKeyField(User,null=False)
    created = models.DateTimeField(default=timezone.now)
    modifier = models.ForeignKeyField(User,null=False)
    modified = models.DateTimeField(default=timezone.now)


class ActiveModel(AuditModel):
    active = models.BooleanField(default=True)
    active_modifier = models.ForeignKeyField(User,null=False)
    active_modified = models.DateTimeField(null=True)
    

class Publisher(ActiveModel):
    name = models.CharField(max_length=32,null=False,primary_key=True)
    category = models.SmallIntegerField(default=MANAGED,choices=CATEGORY_CHOICES)
    comments = models.TextField(null=True)

    def __str__(self):
        return self.name

    class Meta:
        table_name = 'publisher'

class EventType(ActiveModel):
    name = models.CharField(max_length=32,null=False,primary_key=True)
    publisher = models.ForeignKeyField(Publisher,null=False,backref="event_types")
    category = models.SmallIntegerField(default=MANAGED,choices=CATEGORY_CHOICES)
    comments = models.TextField(null=True)
    sample = JSONField(null=True)

    def __str__(self):
        return "{}.{}".format(self.publisher,self.name)

    class Meta:
        table_name = 'event_type'


class Event(BaseModel):
    publisher = models.ForeignKeyField(Publisher,null=False,backref="publisher_events")
    event_type = models.ForeignKeyField(EventType,null=False,backref="events")
    active = models.BooleanField(default=True)
    source = models.CharField(max_length=128,null=False,index=True,unique=False)
    publish_time = models.DateTimeField(default=timezone.now)
    payload = JSONField(null=False)


    def __str__(self):
        return "{}({})".format(self.event_type,self.id)

    class Meta:
        table_name = 'event'

class EventProcessingModule(ActiveModel):
    name = models.CharField(max_length=64,null=False,unique=True)
    code = models.TextField(null=True)
    parameters = models.TextField(null=True)
    comments = models.TextField(null=True)


    def __str__(self):
        return self.name

    class Meta(object):
        db_table = "event_processing_module"


class Subscriber(ActiveModel):
    name = models.CharField(max_length=32,null=False,primary_key=True)
    category = models.SmallIntegerField(default=MANAGED,choices=CATEGORY_CHOICES)
    comments = models.TextField(null=True)

    def __str__(self):
        return self.name

    class Meta:
        table_name = 'subscriber'

class SubscribedEventType(ActiveModel):
    subscriber = models.ForeignKeyField(Subscriber,null=False,backref="event_types")
    publisher = models.ForeignKeyField(Publisher,null=False,backref="subscribed_publisher_event_types")
    event_type = models.ForeignKeyField(EventType,null=False,backref="subscribed_event_types")
    category = models.SmallIntegerField(default=MANAGED,choices=CATEGORY_CHOICES)
    event_processing_module = models.ForeignKeyField(EventProcessingModule,null=True)
    parameters = JSONField(null=True)
    replay_missed_events = models.BooleanField(default=True)
    replay_failed_events = models.BooleanField(default=True)

    last_dispatched_event = models.ForeignKeyField(Event,null=True)
    last_dispatched_time = models.DateTimeField(null=True)
    last_listening_time = models.DateTimeField(null=True)

    @property
    def is_system_event_type(self):
        return self.category == SYSTEM

    @property
    def is_editable(self):
        return self.category in (MANAGED,TESTING)

    @property
    def callback_module(self):
        """
        Return the configured callback if have; otherwise, return None
        throw exception if not configured properly.
        """
        if not hasattr(self,"_callback_module"):
            if not self.event_processing_module or not self.event_processing_module.code:
                #no event processing module, ignore
                return None
            else:
                module_name = "event_{}".format(hashvalue('{}_{}'.format(self.subscriber.name,self.event_type.name)))
                m = imp.new_module(module_name)
                exec(self.event_processing_module.code,m.__dict__)
                if not hasattr(m,"process"):
                    #no event processing method
                    raise Exception("'process' method is not found in event processing module({})".format(self.event_processing_module.name))
                if self.event_processing_module.parameters and not self.parameters:
                    #not configured the parameters, ignore
                    raise Exception("Missing parameters for subscribed event type({})".format(self))
                if self.parameters:
                    for k,v in self.parameters.items():
                        setattr(m,k,v)
                self._callback_module = m

        return self._callback_module

    @property
    def callback(self):
        _module = self.callback_module
        return _module.process if _module else None

    def __str__(self):
        return "{} << {}".format(self.subscriber,self.event_type)

    class Meta:
        table_name = 'subscribed_event_type'


class SubscribedEvent(BaseModel):
    PROCESSING = 0
    SUCCEED = 1
    FAILED = -1
    TIMEOUT = -2

    PROCESSING_TIMEOUT = timedelta(hours=1)
    REPROCESSING_INTERVAL = timedelta(minutes=5)

    subscriber = models.ForeignKeyField(Subscriber,null=False,backref="events")
    publisher = models.ForeignKeyField(Publisher,null=False,backref="subscribed_publisher_events")
    event_type = models.ForeignKeyField(EventType,null=False,backref="subscribed_events")
    event = models.ForeignKeyField(Event,null=True,backref="subscribed")
    process_host = models.CharField(max_length=256,null=False)
    process_pid = models.CharField(max_length=32,null=True)
    process_times = models.IntegerField(default=1)
    process_start_time = models.DateTimeField(default=timezone.now)
    process_end_time = models.DateTimeField(null=True)
    status = models.IntegerField(default=PROCESSING)
    result = models.TextField(null=True)

    class Meta:
        table_name = 'subscribed_event'

class EventProcessingHistory(BaseModel):
    subscribed_event = models.ForeignKeyField(SubscribedEvent,null=False,backref="processing_history")
    process_host = models.CharField(max_length=256,null=False)
    process_pid = models.CharField(max_length=32,null=True)
    process_start_time = models.DateTimeField(default=timezone.now)
    process_end_time = models.DateTimeField(null=True)
    status = models.IntegerField(null=False)
    result = models.TextField(null=True)

    class Meta:
        table_name = 'event_processing_history'

