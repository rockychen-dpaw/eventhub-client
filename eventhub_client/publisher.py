import logging

from eventhub_utils.decorators import (repeat_if_failed,)
from . import settings
from . import models

logger = logging.getLogger(__name__)

class Publisher(object):
    def __init__(self,publisher,event_type):
        self.publisher = publisher
        self.event_type = event_type
        self.host = settings.HOSTNAME

        if isinstance(publisher,models.Publisher):
            self.publisher = publisher
        else:
            self.publisher = models.Publisher.get_or_create(name=publisher,defaults={
                'category':models.PROGRAMMATIC,
                'active':True,
                'active_modifier':models.User.PROGRAMMATIC,
                'active_modified':timezone.now(),
                'modifier':models.User.PROGRAMMATIC,
                'modified':timezone.now(),
                'creator':models.User.PROGRAMMATIC,
                'created':timezone.now(),
            })[0]
        
        if isinstance(event_type,models.EventType):
            self.event_type = event_type
        else:
            self.event_type = models.EventType.get_or_create(name=event_type,defaults={
                "publisher":self.publisher,
                'category':models.PROGRAMMATIC,
                'active':True,
                "sample":None,
                'active_modifier':models.User.PROGRAMMATIC,
                'active_modified':timezone.now(),
                'modifier':models.User.PROGRAMMATIC,
                'modified':timezone.now(),
                'creator':models.User.PROGRAMMATIC,
                'created':timezone.now(),
            })[0]

    @repeat_if_failed(retry=3,retry_interval=1000,retry_message="Waiting {2} milliseconds and then trying to publish again, {0}")
    def publish(self, payload):
        """
        payload
        Return the created event object
        """
        with models.Publisher.database.active_context():
            if self.event_type.sample is None:
                self._update_event_type_sample = False
                self.event_type.sample = payload
                self.event_type.save()
            return models.Event.create(publisher=self.publisher,event_type=self.event_type,source=self.host,payload=payload)

