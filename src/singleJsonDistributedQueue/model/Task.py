from dataclasses import dataclass, field
from uuid import UUID, uuid1


@dataclass
class Task:
    taskId: UUID
    data: str | None
    isStart: bool
    isComplete: bool
    reqTime: float
    lastTaskCheck: float


@dataclass
class TaskIn:
    taskId: UUID = field(default_factory=lambda: uuid1())
    publisherId: UUID = field(default_factory=uuid1)
    data: str | None = field(default=None)
    isStart: bool = field(default=False)
    isComplete: bool = field(default=False)
    reqTime: float = field(default=2.0)
    lastTaskCheck: float = field(default=0.0)

    def toTask(self):
        return Task(
            taskId=self.taskId,
            data=self.data,
            isStart=self.isStart,
            isComplete=self.isComplete,
            reqTime=self.reqTime,
            lastTaskCheck=self.lastTaskCheck,
        )
