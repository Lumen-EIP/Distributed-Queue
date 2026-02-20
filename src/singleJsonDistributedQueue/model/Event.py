from dataclasses import dataclass, field

from singleJsonDistributedQueue.enum.EventOwner import EventOwner
from singleJsonDistributedQueue.enum.EventType import EventType
from singleJsonDistributedQueue.model.Task import TaskIn


@dataclass(frozen=True)
class Event:
    eventType: EventType
    eventOwner: EventOwner
    task: TaskIn | None = field(default=None)
