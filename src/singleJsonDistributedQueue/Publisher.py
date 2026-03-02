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
        self.publisherId = uuid1()
        self.logger = logging.getLogger(f"Publisher-{str(self.publisherId)[:8]}")
        self.logger.setLevel(logging.DEBUG)

        self.logger.debug(f"Initializing Publisher {self.publisherId}...")

        self.brokerManager = brokerManager
        self.publisherConn = self.brokerManager.registerPublisher(self.publisherId)

        self.logger.info("Publisher initialized successfully.")

    async def writeRequest(self, taskDetail: TaskIn):
        self.logger.info(f"Ref-Id: {taskDetail.taskId} | Requesting WRITE operation.")

        newEvent = Event(eventType=EventType.WRITE, eventOwner=EventOwner.PUBLISHER, task=taskDetail)
        await asyncio.to_thread(self.brokerManager.publisherBrokerQueue.put, newEvent)

        self.logger.debug(f"Ref-Id: {taskDetail.taskId} | Awaiting WRITE ACK.")

        await self.waitForAcknowledgement()

        self.logger.info(f"Ref-Id: {taskDetail.taskId} | WRITE operation confirmed.")

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
