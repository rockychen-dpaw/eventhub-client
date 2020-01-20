import time
import traceback

from .publisher import Publisher

from .subscriber import Subscriber
from . import settings
from . import models

from eventhub_utils import timezone

class BaseTest(object):
    def __init__(self,name,desc,database=None,**kwargs):
        """
        kwargs are pub and sub
        """
        self.name = name
        self.desc = desc
        self._database = database or settings.Database.Default.get("test",thread_safe=False)
        self._database.connect(reuse_if_open=True,check_active=True)
        self._connection = self._database.connection()

        self.pubs = set()
        self.subs = set()
        self.subscribes = []
        for k,v in kwargs.items():
            setattr(self,k,v)
            if isinstance(v,Publisher):
                self.pubs.add(v.publisher)
            elif isinstance(v,Subscriber):
                self.subs.add(v.subscriber)
                self.subscribes.append(v)

    @staticmethod
    def print_event(event):
        print("""
Process the event.
ID={} 
Publisher={} 
Event Type={} 
source={} 
publish time={} 
payload={}
""".format(event.id,event.publisher.name,event.event_type.name,event.source,event.publish_time,event.payload))

    def __call__(self):
        try:
            print("")
            print("Run Test({}):{} ...".format(self.name,self.desc))
            self.setup()      
            self.test()
            print("Test({}):{} : Passed".format(self.name,self.desc))
        except:
            traceback.print_exc()
            print("Test({}):{} : Failed".format(self.name,self.desc))
        finally:
            self.tearup()


    def test(self):
        pass

    def setup(self):
        pass
        #for sub in self.subscribes:
        #    sub.start()

    def tearup(self):
        for sub in self.subscribes:
            sub.shutdown(async=True)

        for sub in self.subscribes:
            sub.wait_to_shutdown()

        
        with models.Event.database:
            #delete testing data
            for sub in self.subs:
                models.Event.database.execute_sql("delete from event_processing_history as a using subscribed_event as b where a.subscribed_event_id = b.id and b.subscriber_id = '{}'".format(sub.name))
                models.Event.database.execute_sql("delete from subscribed_event where subscriber_id = '{}'".format(sub.name))
                models.Event.database.execute_sql("delete from subscribed_event_type where subscriber_id = '{}'".format(sub.name))
                models.Event.database.execute_sql("delete from subscriber where name = '{}'".format(sub.name))
                models.Event.database.execute_sql("delete from event_type where publisher_id = 'EventHubConsole' and name = 'sub_{}'".format(sub.name))
    
            for pub in self.pubs:
                models.Event.database.execute_sql("delete from event where publisher_id = '{}'".format(pub.name))
                models.Event.database.execute_sql("delete from event_type where publisher_id = '{}'".format(pub.name))
                models.Event.database.execute_sql("delete from publisher where name = '{}'".format(pub.name))
                models.Event.database.execute_sql("delete from event_type where publisher_id = 'EventHubConsole' and name = 'pub_{}'".format(pub.name))


class SinglePubSubTest(BaseTest):
    def __init__(self,name,desc,database=None):
        with models.Publisher.database:
            pub,created=models.Publisher.get_or_create(name='Pub_Unitest',defaults={
                'category':models.UNITESTING,
                'active':True,
                'commetns':'For unitesting',
                'active_modifier':models.User.PROGRAMMATIC,
                'active_modified':timezone.now(),
                'modifier':models.User.PROGRAMMATIC,
                'modified':timezone.now(),
                'creator':models.User.PROGRAMMATIC,
                'created':timezone.now(),
            })
            event_type,created=models.EventType.get_or_create(name='unitest_event',defaults={
                'publisher':pub,
                'category':models.UNITESTING,
                'active':True,
                'comments':'For unitesting',
                'active_modifier':models.User.PROGRAMMATIC,
                'active_modified':timezone.now(),
                'modifier':models.User.PROGRAMMATIC,
                'modified':timezone.now(),
                'creator':models.User.PROGRAMMATIC,
                'created':timezone.now(),
            })
            sub,created=models.Subscriber.get_or_create(name='Sub_Unitest',defaults={
                'category':models.UNITESTING,
                'active':True,
                'commetns':'For unitesting',
                'active_modifier':models.User.PROGRAMMATIC,
                'active_modified':timezone.now(),
                'modifier':models.User.PROGRAMMATIC,
                'modified':timezone.now(),
                'creator':models.User.PROGRAMMATIC,
                'created':timezone.now(),
            })
        super().__init__(
            name,
            desc,
            pub=Publisher(pub,event_type),
            sub=Subscriber(sub)
        )

class BasicPubSubTest(SinglePubSubTest):
    def __init__(self,name="Basic Pub/Sub Testing",desc="Test basic publish/subscribe event"):
        super().__init__(name,desc)

    def test(self):
        now = timezone.now()
        processed_events = []
        events = ["{}: Hello Eason".format(now),"{}: How are going today.".format(now),"{}: bye".format(now)]#,"{}: bye1".format(now),"{}: bye2".format(now)]
        published_events = {}
        def _process(event):
            self.print_event(event)
            #time.sleep(10)
            processed_events.append(event.id)

        self.sub.subscribe('unitest_event',callback=_process)
        self.sub.start()
    
        for e in events:
            event = self.pub.publish(e)
            print("publish event {}".format(event))
            published_events[event.id] = event
        pending_events = dict(published_events)
    
        waited_times = 0
        while len(pending_events) > 0 and waited_times < 10 * (len(events) + 1):
            for e in processed_events:
                if e in pending_events:
                    del pending_events[e]

            time.sleep(1)
            waited_times += 1
    
        assert len(pending_events) == 0,"Events ({}) are not processed".format(pending_events.values())

        subscribed_events = models.SubscribedEvent.select().where(models.SubscribedEvent.event << [v for v in published_events.values()])
        assert len(events) == len(subscribed_events),"Only {}/{} events were processed".format(len(subscribed_events),len(events))

        succeed_subscribed_events = subscribed_events.where(models.SubscribedEvent.status == models.SubscribedEvent.SUCCEED)
        assert len(events) == len(subscribed_events),"Only {}/{} events were processed successfully".format(len(subscribed_events),len(events))

class FailedProcessingTest(SinglePubSubTest):
    def __init__(self,name="Failed Processing Testing",desc="Test event processing unsuccessfully"):
        super().__init__(name,desc)

    def test(self):
        now = timezone.now()
        events = []
        processed_events = []
        def _process(event):
            self.print_event(event)
            #time.sleep(10)
            processed_events.append(event.id)
            raise Exception("Failed processing testing")
                
        self.sub.subscribe('unitest_event',callback=_process)
        self.sub.start()
    
        events = ["{}: Hello Eason".format(now),"{}: How are going today.".format(now),"{}: bye".format(now)]
        published_events = {}
        for e in events:
            event = self.pub.publish(e)
            published_events[event.id] = event
        pending_events = dict(published_events)
    
        waited_times = 0
        while len(pending_events) > 0 and waited_times < 10 * (len(events) + 1):
            for e in processed_events:
                if e in pending_events:
                    del pending_events[e]

            time.sleep(1)
            waited_times += 1
    
        assert len(pending_events) == 0,"Events ({}) are not processed".format(pending_events.values())

        
        subscribed_events = models.SubscribedEvent.select().where(models.SubscribedEvent.event << [v for v in published_events.values()])
        assert len(events) == len(subscribed_events),"Only {}/{} events were processed".format(len(subscribed_events),len(events))

        failed_subscribed_events = subscribed_events.where(models.SubscribedEvent.status == models.SubscribedEvent.FAILED)
        assert len(events) == len(subscribed_events),"Only {}/{} events were processed unsuccessfully".format(len(subscribed_events),len(events))

def test_all():
    BasicPubSubTest()()
    FailedProcessingTest()()


test_all()

