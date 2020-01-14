import select
import os
import logging
import json
from threading import Thread
import queue
import traceback
import time

from . import settings
from eventhub_utils.decorators import (repeat_if_failed,)
from . import models
from eventhub_utils import timezone

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

NOT_READY = ([], [], [])

class ReplayFailedEventsWorker(Thread):
    def __init__(self,subscriber):
        super().__init__(name="Replay Failed Events Worker {}".format(subscriber.subscriber.name))
        self.subscriber = subscriber
        self._shutdown = False

    def shutdown(self):
        self._shutdown=True
        if self.is_alive():
            self.join()

    @property
    def is_shutdown_requested(self):
        return self._shutdown

    def run(self):
        logger.info("Retrieve Failed Events for {} is running".format(self.subscriber.subscriber.name))
        try:
            waited_seconds = 0
            while not self._shutdown:
                time.sleep(1)
                waited_seconds += 1
                if waited_seconds >= models.SubscribedEvent.PROCESSING_TIMEOUT.seconds:
                    for event_type_name,value in self.subscriber._event_types.items():
                        self.subscriber._replay_failed_events(event_type_name,value[0])
                    waited_seconds = 0
        except KeyboardInterrupt:
            pass
        logger.info("Retrieve Failed Events for {} is end".format(self.subscriber.subscriber.name))

class Listener(Thread):
    def __init__(self,subscriber):
        super().__init__(name="Listener {}".format(subscriber.subscriber.name))
        self.subscriber = subscriber

    def run(self):
        logger.info("Event listener for {} is running".format(self.subscriber.subscriber.name))
        try:
            self.subscriber.listen()
        finally:
            self.subscriber.close()
        logger.info("Event listener for {} is end".format(self.subscriber.subscriber.name))

class Worker(Thread):
    def __init__(self,subscriber,event_type_name):
        super().__init__(name="Worker {}.{} ".format(subscriber.subscriber.name,event_type_name))
        self.subscriber = subscriber
        self.event_type_name = event_type_name
        self._queue = queue.Queue()
        self._shutdown = False

    def run(self):
        logger.info("The worker thread for {}.{} is running".format(self.subscriber.subscriber.name,self.event_type_name))
        while True:
            event = None
            try:
                event = self._queue.get(block=True,timeout=2)
                processed = self.subscriber.process_event(event)
                if not processed:
                    #event is not processed, add to the end of the queue again.
                    self._queue.put(event)
            except queue.Empty:
                if self._shutdown:
                    #no more event to processing,and user already requests to shutdown
                    break
                else:
                    pass
            except KeyboardInterrupt:
                break
            except:
                #failed to process the event,add to the end of the queue again
                logger.error(traceback.format_exc())
                if event:
                    self._queue.put(event)

        logger.info("The worker thread for {}.{} is end".format(self.subscriber.subscriber.name,self.event_type_name))

    def add(self,event):
        self._queue.put(event)

    def shutdown(self):
        self._shutdown=True
        if self.is_alive():
            self.join()

    @property
    def is_shutdown_requested(self):
        return self._shutdown


class Subscriber(object):
    def __init__(self,subscriber,database=None,select_timeout=5,replay_missing_events=True):
        with models.Subscriber.database.active_context():
            self.subscriber,created = models.Subscriber.get_or_create(name=subscriber)

        self._host = settings.HOSTNAME
        self._database = database or settings.Database.Default.get("listen",thread_safe=False)
        self._connection = None
        self._select_timeout = select_timeout
        self._event_types = {}
        self._replay_missing_events = replay_missing_events
        self._listener = Listener(self)
        self._replay_failed_events_worker = ReplayFailedEventsWorker(self)
        self._shutdown = False


    @property
    def started(self):
        return self._listener.is_alive()

    @property
    def connection(self):
        if not self._database.is_active or not self._connection:
            logger.info("Try to connect to database")
            self._database.connect(reuse_if_open=True,check_active=True)
            self._connection = self._database.connection()
            self._connection.autocommit = True

            for event_type_name,value in self._event_types.items():
                self.subscribe(value[0].event_type,value[1],resubscribe=True)

        return self._connection

    def shutdown(self,async=False):
        self._shutdown = True
        if self._replay_failed_events_worker.is_alive():
            self._replay_failed_events_worker.shutdown()

        if not self._listener.is_alive():
            self.close()
        elif not async:
            self._listener.join()

    def wait_to_shutdown(self):
        if self._shutdown:
            if self._listener.is_alive():
                self._listener.join()
        else:
            self.shutdown()


    def _replay_missed_events(self,event_type_name,subscribed_eventtype):
        with models.Event.database.active_context():
            if subscribed_eventtype.last_dispatched_event:
                missing_events = models.Event.select().where(
                    (models.Event.event_type == subscribed_eventtype.event_type) &
                    (models.Event.id > subscribed_eventtype.last_dispatched_event.id)
                )
            else:
                missing_events = models.Event.select().where(
                    (models.Event.event_type == subscribed_eventtype.event_type)
                )

            for event in missing_events:
                self._event_types[event_type_name][2].add(event)

    def _replay_failed_events(self,event_type_name,subscribed_eventtype):
        with models.SubscribedEvent.database.active_context():
            failed_events = models.SubscribedEvent.select().where(
                (models.SubscribedEvent.subscriber == subscribed_eventtype.subscriber) &
                (models.SubscribedEvent.publisher == subscribed_eventtype.publisher) & 
                (models.SubscribedEvent.event_type == subscribed_eventtype.event_type) &
                ( 
                    ((models.SubscribedEvent.status == models.SubscribedEvent.PROCESSING) & (models.SubscribedEvent.process_start_time < timezone.now() - models.SubscribedEvent.PROCESSING_TIMEOUT)) |
                    (models.SubscribedEvent.status < 0)
                )
            )
            for event in failed_events:
                self._event_types[event_type_name][2].add(event)

    def process_event(self,event):
        """
        Return True if processed; return False if already processed or being processed by other process
        """
        with models.SubscribedEvent.database.active_context():
            if not isinstance(event,models.Event):
                event = models.Event.get_by_id(event)

            event_type_name = '{}.{}'.format(event.publisher.name,event.event_type.name)
            #get the processing lock(required when multiple processes are running for the same subscriber.)
            subscribedevent,created = models.SubscribedEvent.get_or_create(
                subscriber=self.subscriber,
                publisher=event.publisher,
                event_type=event.event_type,
                event=event,
                defaults={
                    'process_host':self._host,
                    'process_pid':os.getpid(),
                    'process_times':1,
                    'process_start_time':timezone.now(),
                    'status':models.SubscribedEvent.PROCESSING,
                }
            )
            if not created:
                if subscribedevent.status == models.SubscribedEvent.FAILED:
                    #failed event, process again
                    pass
                elif subscribedevent.status == models.SubscribedEvent.SUCCEED:
                    #processed
                    return True
                elif subscribedevent.status == models.SubscribedEvent.PROCESSING and (timezone.now() - subscribedevent.process_start_time > models.SubscribedEvent.PROCESSING_TIMEOUT):
                    #processed over 1 hour, treat it as failed.
                    pass
                else:
                    #is processing by other process
                    return False

                #get the processing lock
                updated_rows = models.SubscribedEvent.update(
                    process_host = self._host,
                    process_pid = os.getpid(),
                    process_times = subscribedevent.process_times + 1,
                    process_start_time = timezone.now(),
                    process_end_time = None,
                    status = models.SubscribedEvent.PROCESSING,
                    result = None
                ).where(
                    (models.SubscribedEvent.id == subscribedevent.id) & 
                    (models.SubscribedEvent.process_times == subscribedevent.process_times)
                ).execute()

                if not updated_rows:
                    #is processing by other process
                    return False
        
            try:
                now = timezone.now()
    
                if not created:
                    #save the processing history for the failed event before reprocessing.
                    models.EventProcessHistory.create(
                        subscribed_event = subscribedevent,
                        process_host = subscribedevent.process_host,
                        process_pid = subscribedevent.process_pid,
                        process_start_time = subscribedevent.process_start_time,
                        process_end_time = subscribedevent.process_end_time,
                        status = models.SubscribedEvent.TIMEOUT if subscribedevent.status == models.SubscribedEvent.PROCESSING else subscribedevent.status,
                        result = subscribedevent.result
                    )
                    
                #call callback to process the event
                result = self._event_types[event_type_name][1](event)
                #update subscribed event status
                updated_rows = models.SubscribedEvent.update(
                    process_end_time = timezone.now(),
                    status = models.SubscribedEvent.SUCCEED,
                    result = json.dumps(result)
                ).where(
                    (models.SubscribedEvent.id == subscribedevent.id)
                ).execute()
            except:
                #update subscribed event status
                updated_rows = models.SubscribedEvent.update(
                    process_end_time = timezone.now(),
                    status = models.SubscribedEvent.FAILED,
                    result = traceback.format_exc()
                ).where(
                    (models.SubscribedEvent.id == subscribedevent.id)
                ).execute()
    
    
            #update the last dispatched event in SubscribedEventType table
            if created:
                updated_rows = models.SubscribedEventType.update({
                    models.SubscribedEventType.last_dispatched_event : event,
                    models.SubscribedEventType.last_dispatched_time : now,
                }).where(
                    (models.SubscribedEventType.id ==  self._event_types[event_type_name][0]) &
                    (
                        (models.SubscribedEventType.last_dispatched_event >> None) | 
                        (models.SubscribedEventType.last_dispatched_event_id <  event.id)
                    )
                ).execute()

                if updated_rows:
                    #update successfully, update the local object
                    self._event_types[event_type_name][0].last_dispatched_event = event
                    self._event_types[event_type_name][0].last_dispatched_time = now
                else:
                    #other process already update the object, retrieve it from database
                    self._event_types[event_type_name][0] = models.SubscribedEventType.get_by_id(self._event_types[event_type_name][0].id)

        return True

    def subscribe(self,event_type,callback=None,resubscribe=True):
        """
        Return true if subscribed successfully; return False if already subscribed
        """
        with models.EventType.database.active_context():
            if not isinstance(event_type,models.EventType):
                event_type = models.EventType.get_by_id(event_type)

            event_type_name = '{}.{}'.format(event_type.publisher.name,event_type.name)
            if event_type_name in self._event_types and not resubscribe:
                #already subscribed
                return False

            callback = callback or (lambda event:print("""
Publisher={} 
Event Type={} 
source={} 
publish time={} 
payload={}
""".format(event.publisher.name,event.event_type.name,event.source,event.publish_time,event.payload)))

            event_type_name = '{}.{}'.format(event_type.publisher.name,event_type.name)

            subscribed_eventtype,created = models.SubscribedEventType.get_or_create(
                subscriber=self.subscriber,
                publisher=event_type.publisher,
                event_type=event_type,
                defaults={
                    'managed':True,
                    'active':True
                }
            )

            if event_type_name in self._event_types:
                worker = self._event_types[event_type_name][2]
                if not worker or not worker.is_alive():
                    worker = Worker(self,event_type_name)
                    worker.start()
                elif worker.is_shutdown_requested:
                    worker.join()
                    worker = Worker(self,event_type_name)
                    worker.start()
            else:
                worker = Worker(self,event_type_name)
                worker.start()

            #try to connect to database, this maybe trigger a reregister for all event_types in _event_types if connection to database is not established before
            self.connection
            if event_type_name in self._event_types:
                self._event_types[event_type_name][1] = callback
                self._event_types[event_type_name][2] = worker
            else:
                self._event_types[event_type_name] = [subscribed_eventtype,callback,worker]
            self._replay_failed_events(event_type_name,subscribed_eventtype)
            self._replay_missed_events(event_type_name,subscribed_eventtype)
            
            with self.connection.cursor() as cur:
                #listen pg notification
                cur.execute('LISTEN "{}";'.format(event_type_name))

        return True

    def unsubscribe(self,event_type,remove=True):
        """
        Return true if unsubscribed successfully; return False if not subscribed before
        """
        try:
            with models.EventType.database.active_context():
                if not isinstance(event_type,models.EventType):
                    event_type = models.EventType.get_by_id(event_type)

                event_type_name = '{}.{}'.format(event_type.publisher.name,event_type.name)
                if event_type_name not in self._event_types:
                    #not subscribed
                    return False

                with self.connection.cursor() as cur:
                    cur.execute('UNLISTEN "{}";'.format(event_type_name))
        except:
            pass

        #shutdown the worker thread
        self._event_types[event_type_name][2].shutdown()

        if remove:
            del self._event_types[event_type_name]

        return True


    def start(self):
        self._listener.start()
        self._replay_failed_events_worker.start()

    @repeat_if_failed(retry=-1,retry_interval=2000,retry_message="Waiting {2} milliseconds and then trying to listen again, {0}")
    def listen(self):
        while not self._shutdown:
            try:
                if select.select([self.connection], [], [], self._select_timeout) == NOT_READY:
                    pass
                else:
                    self.connection.poll()
                    with models.EventType.database.active_context():
                        while self.connection.notifies:
                            notify_event = self.connection.notifies.pop(0)
                            event_type_name = notify_event.channel
                            if event_type_name not in self._event_types:
                                #not listening this event type. skip
                                logger.info("The subscriber({}) is not listening this event type ({}), skip the event({}).".format(self.subscriber,event_type_name,json.dumps(notify_event)))
                                continue
                            #get the event object
                            notify_payload = json.loads(notify_event.payload)
                            self._event_types[event_type_name][2].add(notify_payload['id'])
            except:
                #check whether the connection is broken or not
                self._database.clean_if_inactive()
                raise
                     

    def close(self):
        for v in self._event_types.values():
            self.unsubscribe(v[0].event_type,remove=False)
        self._connection = None
        self._database.close()

        self._listener = Listener(self)
        self._replay_failed_events_worker = ReplayFailedEventsWorker(self)
