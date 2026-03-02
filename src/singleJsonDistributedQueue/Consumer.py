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
        self.logger = logging.getLogger(name=__name__)
        self.logger.setLevel(logging.DEBUG)

        self.consumerId: UUID = uuid1()
        self.logger.info(msg=f"Consumer: {self.consumerId} initializing....")

        self.brokerManager = brokerManager

        self.consumerConn, self.consumerTaskConn = self.brokerManager.registerConsumer(
            consumerId=self.consumerId
        )

        self.runningConsumer = asyncio.create_task(self.waitForTask())

        self.logger.info(msg=f"Consumer: {self.consumerId} successfully Initialized")

    async def writeRequest(self, taskDetail: Task):
        self.logger.info(msg=f"Sending A Write Request for task: {taskDetail.taskId}")

        newEvent = Event(eventType=EventType.WRITE, eventOwner=EventOwner.CONSUMER, task=taskDetail)
        await asyncio.to_thread(self.brokerManager.consumerBrokerQueue.put, newEvent)

        self.logger.info(msg=f"Waiting for Write Acknowledgement of task: {taskDetail.taskId}")

        await self.waitForAcknowledgement()

        self.logger.info(msg=f"Task: {taskDetail.taskId} Successfully Written")

    async def readRequest(self, taskDetail: Task):
        self.logger.info(msg=f"Sending A Read Request for task: {taskDetail.taskId}")

        newEvent = Event(eventType=EventType.READ, eventOwner=EventOwner.CONSUMER, task=taskDetail)
        await asyncio.to_thread(self.brokerManager.consumerBrokerQueue.put, newEvent)

    async def waitForTask(self):
        self.logger.info(msg=f"Consumer: {self.consumerId} waiting for task")

        while True:
            try:
                has_data = await asyncio.to_thread(self.consumerTaskConn.poll, 0.5)
                if has_data:
                    taskDetail = await asyncio.to_thread(self.consumerTaskConn.recv)
                    self.logger.info(msg=f"Consumer: {self.consumerId} got a task: {taskDetail.taskId}")

                    await self.processTask(taskDetail=taskDetail)
            except CancelledError:
                return

    async def processTask(self, taskDetail: Task):
        self.logger.info(msg=f"Task: {taskDetail.taskId} Started By Consumer: {self.consumerId}")

        reqTime = taskDetail.reqTime
        while reqTime > 0:
            reqTime -= 0.5
            await asyncio.sleep(0.5)
        taskDetail.isComplete = True
        await self.writeRequest(taskDetail=taskDetail)

        self.logger.info(msg=f"Task: {taskDetail.taskId} Finished By Consumer: {self.consumerId}")

        await asyncio.to_thread(self.brokerManager.consumerWaitingQueue.put, self.consumerId)
        self.logger.info(msg=f"Consumer: {self.consumerId} Going Back to Waiting Queue")
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
        self.logger.info(msg=f"Consumer: {self.consumerId} About to Shutdown....")
        if self.runningConsumer is not None:
            self.runningConsumer.cancel()
            await self.runningConsumer
        self.logger.info(msg=f"Consumer: {self.consumerId} Successfully Shutdown")
