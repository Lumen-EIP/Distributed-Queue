import asyncio
import logging
from asyncio import CancelledError
from uuid import UUID, uuid1

from singleJsonDistributedQueue.broker.BrokerManager import BrokerManager
from singleJsonDistributedQueue.enum.EventOwner import EventOwner
from singleJsonDistributedQueue.enum.EventType import EventType
from singleJsonDistributedQueue.model.Event import Event
from singleJsonDistributedQueue.model.Task import Task


class Consumer:
    def __init__(self, brokerManager: BrokerManager):
        self.consumerId: UUID = uuid1()
        self.logger = logging.getLogger(f"Consumer-{str(self.consumerId)[:8]}")
        self.logger.setLevel(logging.DEBUG)

        self.logger.debug(f"Initializing Consumer {self.consumerId}...")

        self.brokerManager = brokerManager

        self.consumerConn, self.consumerTaskConn = self.brokerManager.registerConsumer(
            consumerId=self.consumerId
        )

        self.runningConsumer = asyncio.create_task(self.waitForTask())

        self.logger.info("Consumer initialized successfully and listening.")

    async def writeRequest(self, taskDetail: Task):
        self.logger.info(f"Ref-Id: {taskDetail.taskId} | Requesting WRITE operation.")

        newEvent = Event(eventType=EventType.WRITE, eventOwner=EventOwner.CONSUMER, task=taskDetail)
        await asyncio.to_thread(self.brokerManager.consumerBrokerQueue.put, newEvent)

        self.logger.debug(f"Ref-Id: {taskDetail.taskId} | Awaiting WRITE ACK.")

        await self.waitForAcknowledgement()

        self.logger.info(f"Ref-Id: {taskDetail.taskId} | WRITE operation confirmed.")

    async def readRequest(self, taskDetail: Task):
        self.logger.info(f"Ref-Id: {taskDetail.taskId} | Requesting READ operation.")

        newEvent = Event(eventType=EventType.READ, eventOwner=EventOwner.CONSUMER, task=taskDetail)
        await asyncio.to_thread(self.brokerManager.consumerBrokerQueue.put, newEvent)

    async def waitForTask(self):
        self.logger.debug("Entering task wait loop...")

        while True:
            try:
                has_data = await asyncio.to_thread(self.consumerTaskConn.poll, 0.5)
                if has_data:
                    taskDetail = await asyncio.to_thread(self.consumerTaskConn.recv)
                    self.logger.info(f"Ref-Id: {taskDetail.taskId} | Task received.")

                    await self.processTask(taskDetail=taskDetail)
            except CancelledError:
                return

    async def processTask(self, taskDetail: Task):
        self.logger.info(
            f"Ref-Id: {taskDetail.taskId} | Processing started (Duration: {taskDetail.reqTime}s)."
        )

        reqTime = taskDetail.reqTime
        while reqTime > 0:
            reqTime -= 0.5
            await asyncio.sleep(0.5)
        taskDetail.isComplete = True
        await self.writeRequest(taskDetail=taskDetail)

        self.logger.info(f"Ref-Id: {taskDetail.taskId} | Processing finished.")

        await asyncio.to_thread(self.brokerManager.consumerWaitingQueue.put, self.consumerId)
        self.logger.debug("Re-joined waiting queue.")
        return True

    def aliveAcknowledgement(self):
        pass

    def checkBrokerAvailablity(self):
        pass

    async def waitForAcknowledgement(self):
        while True:
            try:
                has_data = await asyncio.to_thread(self.consumerConn.poll, 0.5)
                if has_data:
                    isAck = await asyncio.to_thread(self.consumerConn.recv)
                    if isAck:
                        break
            except CancelledError:
                return

    async def close(self):
        self.logger.info("Shutting down Consumer.")
        if self.runningConsumer is not None:
            self.runningConsumer.cancel()
            await self.runningConsumer
        self.logger.info(msg=f"Consumer: {self.consumerId} Successfully Shutdown")
