import asyncio
import logging
import time
from asyncio import CancelledError
from uuid import uuid1

from singleJsonDistributedQueue.broker.BrokerManager import BrokerManager
from singleJsonDistributedQueue.enum.EventOwner import EventOwner
from singleJsonDistributedQueue.enum.EventType import EventType
from singleJsonDistributedQueue.model.Event import Event
from singleJsonDistributedQueue.model.Task import TaskIn


class Publisher:
    def __init__(self, brokerManager: BrokerManager):
        self.logger = logging.getLogger(name=__name__)
        self.logger.setLevel(logging.DEBUG)

        self.publisherId = uuid1()
        self.logger.info(msg=f"Publisher: {self.publisherId} initializing....")

        self.brokerManager = brokerManager
        self.publisherConn = self.brokerManager.registerPublisher(self.publisherId)

        self.logger.info(msg=f"Publisher: {self.publisherId} successfully Initialized")

    async def writeRequest(self, taskDetail: TaskIn):
        self.logger.info(msg=f"Publisher: {self.publisherId} Sending A Write Request for task: {taskDetail.taskId}")

        newEvent = Event(eventType=EventType.WRITE, eventOwner=EventOwner.PUBLISHER, task=taskDetail)
        await asyncio.to_thread(self.brokerManager.publisherBrokerQueue.put, newEvent)

        self.logger.info(msg=f"Publisher: {self.publisherId} Waiting for Write Acknowledgement of task: {taskDetail.taskId}")

        await self.waitForAcknowledgement()

        self.logger.info(msg=f"Task: {taskDetail.taskId} Successfully Written by Publisher: {self.publisherId}")

    async def readRequest(self, taskDetail: TaskIn):
        newEvent = Event(eventType=EventType.READ, eventOwner=EventOwner.PUBLISHER, task=taskDetail)
        await asyncio.to_thread(self.brokerManager.publisherBrokerQueue.put, newEvent)

    async def waitForAcknowledgement(self):
        start = time.monotonic()
        timeout = 10

        while True:
            if time.monotonic() - start > timeout:
                raise TimeoutError("ACK timeout")
            try:
                has_data = await asyncio.to_thread(self.publisherConn.poll, 0.5)
                if has_data:
                    isAck = await asyncio.to_thread(self.publisherConn.recv)
                    if isAck:
                        break
            except CancelledError:
                return

    def receiveTask(self):
        pass

    def aliveAcknowledgement(self):
        pass

    def checkBrokerAvailablity(self):
        pass
