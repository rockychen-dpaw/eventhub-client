import time
import traceback

from .publisher import Publisher

from .subscriber import Subscriber
from . import settings
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

    def print_event(event):
        print("""
Process the event.
Publisher={} 
Event Type={} 
source={} 
publish time={} 
payload={}
""".format(event.publisher.name,event.event_type.name,event.source,event.publish_time,event.payload))

    def __call__(self):
        try:
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
        for sub in self.subscribes:
            sub.start()

    def tearup(self):
        for sub in self.subscribes:
            sub.shutdown(async=True)

        for sub in self.subscribes:
            sub.wait_to_shutdown()

        
        with self._connection.cursor() as cur:
            #delete testing data
            for sub in self.subs:
                cur.execute("delete from event_processing_history as a using subscribed_event as b where a.subscribed_event_id = b.id and b.subscriber_id = '{}'".format(sub.name))
                cur.execute("delete from subscribed_event where subscriber_id = '{}'".format(sub.name))
                cur.execute("delete from subscribed_event_type where subscriber_id = '{}'".format(sub.name))
                cur.execute("delete from subscriber where name = '{}'".format(sub.name))
                cur.execute("delete from event_type where publisher_id = 'EventHubConsole' and name = 'sub_{}'".format(sub.name))
    
            for pub in self.pubs:
                cur.execute("delete from event where publisher_id = '{}'".format(sub.name))
                cur.execute("delete from event_type where publisher_id = '{}'".format(sub.name))
                cur.execute("delete from publisher where publisher_id = '{}'".format(sub.name))
                cur.execute("delete from event_type where publisher_id = 'EventHubConsole' and name = 'pub_{}'".format(pub.name))


class SinglePubSubTest(BaseTest):
    def __init__(self,name,desc,database=None):
        super().__init__(name,desc,pub=Publisher('Pub_Test','test_event'),sub=Subscriber('Sub_Test'))

class BasicPubSubTest(SinglePubSubTest):
    def __init__(self):
        super().__init__("Basic Pub/Sub Testing","Test basic publish/subscribe event")
    def test(self):
        now = timezone.now()
        events = []
        unknown_events=[]
        def _process(event):
            if event.publisher == self.pub.publisher and event.event_type == self.pub.event_type and event.payload == events[0]:
                del events[0]
                self.print_event(event)
            else:
                unknown_events.append(event)
    
                
        self.sub.subscribe('test_event',callback=_process)
    
        events = ["{}: Hello Eason".format(now),"{}: How are going today.".format(now)]
        for e in events:
            self.pub.publish(e)
    
        waited_times = 0
        while len(events) > 0 and waited_times < 20:
            time.sleep(1)
            waited_times += 1
    
    
        assert len(events) == 0,"Events ({}) are not processed".format(events)
        assert len(unknown_events) == 0,"Found unknown events.{}".format(unknown_events)

def test_all():
    BasicPubSubTest()()



test_all()

