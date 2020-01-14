from datetime import timedelta

import peewee as models
from playhouse.postgres_ext import JSONField

from eventhub_utils import timezone,cachedclassproperty,classproperty

from . import settings

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

class Publisher(BaseModel):
    name = models.CharField(max_length=32,null=False,primary_key=True)
    active = models.BooleanField(default=True)
    comments = models.TextField(null=True)
    register_time = models.DateTimeField(default=timezone.now)

    def __str__(self):
        return self.name

    class Meta:
        table_name = 'publisher'

class EventType(BaseModel):
    name = models.CharField(max_length=32,null=False,primary_key=True)
    publisher = models.ForeignKeyField(Publisher,null=False,backref="event_types")
    managed = models.BooleanField(default=True)
    active = models.BooleanField(default=True)
    comments = models.TextField(null=True)
    sample = JSONField(null=True)
    register_time = models.DateTimeField(default=timezone.now)

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

class Subscriber(BaseModel):
    name = models.CharField(max_length=32,null=False,primary_key=True)
    active = models.BooleanField(default=True)
    comments = models.TextField(null=True)
    register_time = models.DateTimeField(default=timezone.now)

    def __str__(self):
        return self.name

    class Meta:
        table_name = 'subscriber'

class SubscribedEventType(BaseModel):
    subscriber = models.ForeignKeyField(Subscriber,null=False,backref="event_types")
    publisher = models.ForeignKeyField(Publisher,null=False,backref="subscribed_publisher_event_types")
    event_type = models.ForeignKeyField(EventType,null=False,backref="subscribed_event_types")
    managed = models.BooleanField(default=True)
    active = models.BooleanField(default=True)
    last_dispatched_event = models.ForeignKeyField(Event,null=True)
    last_dispatched_time = models.DateTimeField(null=True)
    register_time = models.DateTimeField(default=timezone.now)

    def __str__(self):
        return "{} subscribes {}".format(self.subscriber,self.event_type)

    class Meta:
        table_name = 'subscribed_event_type'


class SubscribedEvent(BaseModel):
    PROCESSING = 0
    SUCCEED = 1
    FAILED = -1
    TIMEOUT = -2

    PROCESSING_TIMEOUT = timedelta(hours=1)

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

