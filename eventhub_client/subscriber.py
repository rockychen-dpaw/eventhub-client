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
        super().__init__(name="Replay Failed Events Worker {}".format(subscriber.subscriber.name),daemon=False)
        self.subscriber = subscriber
        self._shutdown = False
        self._running = None

    def shutdown(self):
        self._shutdown=True
        if self.is_alive():
            self.join()

    @property
    def is_shutdown_requested(self):
        return self._shutdown

    def is_alive(self):
        return True if self._running else False

    def join(self):
        while self._running:
            time.sleep(0.1)

    def run(self):
        self._running = True
        logger.info("Retrieve Failed Events for {} is running".format(self.subscriber.subscriber.name))
        try:
            waited_seconds = 0
            while not self._shutdown:
                time.sleep(1)
                waited_seconds += 1
                if waited_seconds >= models.SubscribedEvent.REPROCESSING_INTERVAL.seconds:
                    for event_type_name,value in self.subscriber._event_types.items():
                        self.subscriber._replay_failed_events(event_type_name,value[0])
                    waited_seconds = 0
        except KeyboardInterrupt:
            pass
        logger.info("Retrieve Failed Events for {} is end".format(self.subscriber.subscriber.name))
        self._running = False

class Listener(Thread):
    def __init__(self,subscriber):
        super().__init__(name="Listener {}".format(subscriber.subscriber.name),daemon=False)
        self.subscriber = subscriber
        self._running = None

    def is_alive(self):
        return True if self._running else False

    def join(self):
        while self._running:
            time.sleep(0.1)

    def run(self):
        self._running = True
        logger.info("Event listener for {} is running".format(self.subscriber.subscriber.name))
        try:
            self.subscriber.listen()
        finally:
            self.subscriber.close()
        logger.info("Event listener for {} is end".format(self.subscriber.subscriber.name))
        self._running = False

class Worker(Thread):
    def __init__(self,subscriber,event_type_name):
        super().__init__(name="Worker {}.{} ".format(subscriber.subscriber.name,event_type_name),daemon=False)
        self.subscriber = subscriber
        self.event_type_name = event_type_name
        self._queue = queue.Queue()
        self._shutdown = False
        self._running = None

    def is_alive(self):
        return True if self._running else False

    def join(self):
        while self._running:
            time.sleep(0.1)

    def run(self):
        self._running = True
        logger.info("The worker thread for {}->{} is running".format(self.subscriber.subscriber.name,self.event_type_name))
        while True:
            event = None
            try:
                event = self._queue.get(block=True,timeout=2)
                logger.debug("Got Event({} for )({}->{})".format(event,self.subscriber.subscriber.name,self.event_type_name))
                processed = self.subscriber.process_event(event)
                if not processed:
                    #event is not processed, add to the end of the queue again.
                    self._queue.put(event)
            except queue.Empty:
                #logger.debug("Event queue({}->{}) is empty,shutdown={}".format(self.subscriber.subscriber.name,self.event_type_name,self._shutdown))
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

        logger.info("The worker thread for {}->{} is end".format(self.subscriber.subscriber.name,self.event_type_name))
        self._running = False

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
    def __init__(self,subscriber,database=None,select_timeout=5,process_missed_events=True,category=models.PROGRAMMATIC):
        if isinstance(subscriber,models.Subscriber):
            self.subscriber = subscriber
        elif category == models.MANAGED:
            with models.Subscriber.database.active_context():
                self.subscriber = models.Subscriber.get_by_id(subscriber)
        else:
            with models.Subscriber.database.active_context():
                self.subscriber,created = models.Subscriber.get_or_create(name=subscriber,defaults={
                    'category':category,
                    'active':True,
                    'active_modifier':models.User.PROGRAMMATIC,
                    'active_modified':timezone.now(),
                    'modifier':models.User.PROGRAMMATIC,
                    'modified':timezone.now(),
                    'creator':models.User.PROGRAMMATIC,
                    'created':timezone.now(),
                })

        self._host = settings.HOSTNAME
        self._database = database or settings.Database.Default.get("listener_{}".format(self.subscriber.name),thread_safe=False)
        self._connection = None
        self._select_timeout = select_timeout
        self._event_types = {}
        self._process_missed_events = process_missed_events
        #automatically listen to managed events
        with models.SubscribedEventType.database.active_context():
            managed_event_types = models.SubscribedEventType.select().where(
                (models.SubscribedEventType.subscriber == self.subscriber) &
                (models.SubscribedEventType.active == True) &
                (models.SubscribedEventType.category == models.MANAGED) 
            )
            for event_type in managed_event_types:
                if not event_type.callback:
                    #no event processing module, ignore
                    continue
                try:
                    if not event_type.callback:
                        #no event processing module, ignore
                        continue
                    self.subscribe(event_type,event_type.callback,auto_subscribe=True)

                except:
                    logger.error(traceback.format_exc())

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
                self.subscribe(value[0].event_type,value[1],resubscribe=True,auto_subscribe=True)

        return self._connection

    def shutdown(self,async=False):
        self._shutdown = True
        if self._replay_failed_events_worker.is_alive():
            self._replay_failed_events_worker.shutdown()

        if not self._listener.is_alive():
            self.close()
        elif not async:
            if self._listener.is_alive():
                self._listener.join()

    def wait_to_shutdown(self):
        if self._shutdown:
            if self._listener.is_alive():
                self._listener.join()
        else:
            self.shutdown()


    def _replay_missed_events(self,event_type_name,subscribed_event_type):
        if not subscribed_event_type.replay_missed_events:
            return
        with models.Event.database.active_context():
            if subscribed_event_type.last_dispatched_event:
                missing_events = models.Event.select().where(
                    (models.Event.event_type == subscribed_event_type.event_type) &
                    (models.Event.id > subscribed_event_type.last_dispatched_event.id)
                )
            else:
                missing_events = models.Event.select().where(
                    (models.Event.event_type == subscribed_event_type.event_type)
                )

            for event in missing_events:
                self._event_types[event_type_name][2].add(event)

    def _replay_failed_events(self,event_type_name,subscribed_event_type):
        if not subscribed_event_type.replay_failed_events:
            return
        with models.SubscribedEvent.database.active_context():
            if subscribed_event_type.replay_missed_events:
                failed_events = models.SubscribedEvent.select().where(
                    (models.SubscribedEvent.subscriber == subscribed_event_type.subscriber) &
                    (models.SubscribedEvent.publisher == subscribed_event_type.publisher) & 
                    (models.SubscribedEvent.event_type == subscribed_event_type.event_type) &
                    ( 
                        ((models.SubscribedEvent.status == models.SubscribedEvent.PROCESSING) & (models.SubscribedEvent.process_start_time < timezone.now() - models.SubscribedEvent.PROCESSING_TIMEOUT)) |
                        (models.SubscribedEvent.status < 0)
                    )
                )
            else:
                failed_events = models.SubscribedEvent.select().where(
                    (models.SubscribedEvent.subscriber == subscribed_event_type.subscriber) &
                    (models.SubscribedEvent.publisher == subscribed_event_type.publisher) & 
                    (models.SubscribedEvent.event_type == subscribed_event_type.event_type) &
                    (models.SubscribedEvent.process_start_time > subscribed_event_type.last_listening_time) &
                    ( 
                        ((models.SubscribedEvent.status == models.SubscribedEvent.PROCESSING) & (models.SubscribedEvent.process_start_time < timezone.now() - models.SubscribedEvent.PROCESSING_TIMEOUT)) |
                        (models.SubscribedEvent.status < 0)
                    )
                )
            for event in failed_events:
                self._event_types[event_type_name][2].add(event.event)

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
                    #is processing by other process,treat it as processed
                    return True

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
                    #is processing by other process,treat it as processed
                    return True
        
            try:
                now = timezone.now()
    
                if not created:
                    #save the processing history for the failed event before reprocessing.
                    models.EventProcessingHistory.create(
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

    def subscribed(self,event_type):
        if isinstance(event_type,models.SubscribedEventType):
            event_type = subscribed_event_type.event_type
        elif not isinstance(event_type,models.EventType):
            with models.EventType.database.active_context():
                event_type = models.EventType.get_by_id(event_type)

        event_type_name = '{}.{}'.format(event_type.publisher.name,event_type.name)

        return event_type_name in self._event_types

    @property
    def has_subscription(self):
        return True if self._event_types else False

    def subscribe(self,event_type,callback=None,resubscribe=True,auto_subscribe=False):
        """
        Return (SubscribedEventType,True) if subscribed successfully; return (SubscribedEventType,False) if already subscribed
        """
        with models.EventType.database.active_context():
            if isinstance(event_type,models.SubscribedEventType):
                subscribed_event_type = event_type
                event_type = subscribed_event_type.event_type
            else:
                if not isinstance(event_type,models.EventType):
                    event_type = models.EventType.get_by_id(event_type)

                subscribed_event_type,created = models.SubscribedEventType.get_or_create(
                    subscriber=self.subscriber,
                    publisher=event_type.publisher,
                    event_type=event_type,
                    defaults={
                        'category':self.subscriber.category,
                        'active':True,
                        'active_modifier':models.User.PROGRAMMATIC,
                        'active_modified':timezone.now(),
                        'modifier':models.User.PROGRAMMATIC,
                        'modified':timezone.now(),
                        'creator':models.User.PROGRAMMATIC,
                        'created':timezone.now(),
                    }
                )

            event_type_name = '{}.{}'.format(event_type.publisher.name,event_type.name)
            if event_type_name in self._event_types and not resubscribe:
                #already subscribed
                return (subscribed_event_type,False)

            if auto_subscribe:
                if not callback:
                    raise Exception("Missing callback for auto subscribed event type({})".format(subscribed_event_type))
            elif subscribed_event_type.category == models.PROGRAMMATIC:
                if not callback:
                    raise Exception("Missing callback for programmatic subscribed event type({})".format(subscribed_event_type))
            elif subscribed_event_type.category == models.MANAGED:
                callback = subscribed_event_type.callback
                if not callback:
                    raise Exception("Missing callback for programmatic subscribed event type({})".format(subscribed_event_type))
            else:
                callback = callback or subscribed_event_type.callback
                if not callback:
                    callback = (lambda event:print("""
Publisher={} 
Event Type={} 
source={} 
publish time={} 
payload={}
""".format(event.publisher.name,event.event_type.name,event.source,event.publish_time,event.payload)))

            if event_type_name in self._event_types:
                worker = self._event_types[event_type_name][2]
                if not worker or not worker.is_alive():
                    worker = Worker(self,event_type_name)
                    worker.start()
                elif worker.is_shutdown_requested:
                    if worker.is_alive():
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
                self._event_types[event_type_name] = [subscribed_event_type,callback,worker]

            if subscribed_event_type.replay_missed_events:
                #replay failed event only if replay missed events is enabled
                self._replay_failed_events(event_type_name,subscribed_event_type)
            self._replay_missed_events(event_type_name,subscribed_event_type)
            
            models.SubscribedEventType.update(
                last_listening_time = timezone.now()
            ).where(
                (models.SubscribedEventType.id == subscribed_event_type.id)
            ).execute()
            with self.connection.cursor() as cur:
                #listen pg notification
                cur.execute('LISTEN "{}";'.format(event_type_name))
            logger.info("Listen to {}".format(event_type_name))

        return (subscribed_event_type,True)

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
            logger.info("Stop listen to {}".format(event_type_name))
        except:
            pass

        #shutdown the worker thread
        self._event_types[event_type_name][2].shutdown()

        if remove:
            del self._event_types[event_type_name]

        return True

    @property
    def started(self):
        return self._listener and self._listener.is_alive()

    def start(self):
        self._shutdown = False
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
                    while self.connection.notifies:
                        notify_event = self.connection.notifies.pop(0)
                        event_type_name = notify_event.channel
                        logger.debug("{}:{} in {}".format(event_type_name,notify_event,self._event_types.keys()))
                        
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
