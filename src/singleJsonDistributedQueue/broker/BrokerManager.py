import asyncio
import logging
import multiprocessing
from asyncio import CancelledError
from multiprocessing import Queue
from multiprocessing.connection import PipeConnection
from queue import Empty
from typing import Dict, Tuple
from uuid import UUID

from filelock import BaseAsyncFileLock

from singleJsonDistributedQueue.broker.ConsumerBroker import ConsumerBroker
from singleJsonDistributedQueue.broker.PublisherBroker import PublisherBroker
from singleJsonDistributedQueue.enum.EventOwner import EventOwner
from singleJsonDistributedQueue.enum.EventType import EventType
from singleJsonDistributedQueue.model.Event import Event
from singleJsonDistributedQueue.model.Task import Task


class BrokerManager:
    def __init__(self, jsonQueueLock: BaseAsyncFileLock):
        self.logger = logging.getLogger(name=__name__)
        self.logger.setLevel(logging.DEBUG)

        self.logger.info(msg="Broker Manager Initializing....")

        self.publisherBrokerQueue: Queue[Event] = multiprocessing.Queue()
        self.consumerBrokerQueue: Queue[Event] = multiprocessing.Queue()

        self.acknowledgementQueue: Queue[Tuple[UUID, EventOwner]] = multiprocessing.Queue()

        self.consumerWaitingQueue: Queue[UUID] = multiprocessing.Queue()

        self.publisherMap: Dict[UUID, PipeConnection] = {}
        self.consumerMap: Dict[UUID, PipeConnection] = {}
        self.consumerTaskMap: Dict[UUID, PipeConnection] = {}

        self.consumerTaskMapQueue: Queue[Tuple[UUID, Task]] = multiprocessing.Queue()

        self.publisherBrokerProcess = multiprocessing.Process(
            target=PublisherBroker.initPublisherBroker,
            args=(self.publisherBrokerQueue, self.acknowledgementQueue, jsonQueueLock),
        )
        self.consumerBrokerProcess = multiprocessing.Process(
            target=ConsumerBroker.initConsumerBroker,
            args=(
                self.consumerBrokerQueue,
                self.acknowledgementQueue,
                self.consumerWaitingQueue,
                self.consumerTaskMapQueue,
                jsonQueueLock,
            ),
        )

        self.ackTask: asyncio.Task | None = None

        self.logger.info(msg="Broker Manager Successfully Initialized")

    async def run(self):
        self.logger.info(msg="Broker Manager Starting....")

        self.publisherBrokerProcess.start()
        self.consumerBrokerProcess.start()
        self.ackTask = asyncio.create_task(self.listenAcknowledgement())
        self.assignTask = asyncio.create_task(self.listenTaskAssignment())

    async def close(self):
        self.logger.info(msg="Broker Manager Closing....")

        await self.shutdownBroker()

        if self.ackTask is not None:
            self.ackTask.cancel()
            await self.ackTask

        if self.assignTask is not None:
            self.assignTask.cancel()
            await self.assignTask

        self.logger.info(msg="Broker Manager Successfully Closed")

    async def listenAcknowledgement(self):
        self.logger.info(msg="Broker Manager Started to listen acknowledgements....")

        while True:
            try:
                (receiverId, receiverType) = await asyncio.to_thread(self.acknowledgementQueue.get, True, 1)
                conn = None
                if receiverType is EventOwner.CONSUMER:
                    conn = self.consumerMap.get(receiverId)
                elif receiverType is EventOwner.PUBLISHER:
                    conn = self.publisherMap.get(receiverId)
                if conn is not None:
                    conn.send(True)
                else:
                    self.logger.warning(f"ACK received for unknown publisher/consumer {receiverId}")
            except Empty:
                pass
            except CancelledError:
                break

    async def listenTaskAssignment(self):
        self.logger.info(msg="Broker Manager Started to listen task assignments....")

        while True:
            try:
                (consumerId, assignedTask) = await asyncio.to_thread(self.consumerTaskMapQueue.get, True, 1)
                conn = self.consumerTaskMap[consumerId]
                if conn is not None:
                    conn.send(assignedTask)
                    self.logger.info(f"Task Successfully Send to Consumer: {consumerId}")
                else:
                    self.logger.warning(f"Task received for unknown consumer {consumerId}")
            except Empty:
                pass
            except CancelledError:
                break

    async def checkBrokerStatus(self):
        try:
            while True:
                if self.publisherBrokerProcess is None or not self.publisherBrokerProcess.is_alive():
                    self.publisherBrokerProcess.close()
                    await self.run()

                await asyncio.sleep(1)
        except Exception:
            raise Exception

    async def shutdownBroker(self):
        self.logger.info(msg="Publisher Broker About To Shutdown")
        if self.publisherBrokerProcess and self.publisherBrokerProcess.is_alive():
            await asyncio.to_thread(
                self.publisherBrokerQueue.put,
                Event(eventType=EventType.SHUTDOWN, eventOwner=EventOwner.BROKER_MANAGER),
            )

        self.logger.info(msg="Consumer Broker About To Shutdown")
        if self.consumerBrokerProcess and self.consumerBrokerProcess.is_alive():
            await asyncio.to_thread(
                self.consumerBrokerQueue.put,
                Event(eventType=EventType.SHUTDOWN, eventOwner=EventOwner.BROKER_MANAGER),
            )

        if self.publisherBrokerProcess:
            await asyncio.to_thread(self.publisherBrokerProcess.join, 5)

            if self.publisherBrokerProcess.is_alive():
                self.logger.warning("Publisher Broker did not exit, terminating...")
                self.publisherBrokerProcess.terminate()
                await asyncio.to_thread(self.publisherBrokerProcess.join)
        self.logger.info(msg="Publisher Broker Successfully Shutdown")

        if self.consumerBrokerProcess:
            await asyncio.to_thread(self.consumerBrokerProcess.join, 5)

            if self.consumerBrokerProcess.is_alive():
                self.logger.warning("Consumer Broker did not exit, terminating...")
                self.consumerBrokerProcess.terminate()
                await asyncio.to_thread(self.consumerBrokerProcess.join)
        self.logger.info(msg="Consumer Broker Successfully Shutdown")

    def registerPublisher(self, publisherId: UUID):
        publisherConn, brokerConn = multiprocessing.Pipe()
        self.publisherMap[publisherId] = brokerConn
        return publisherConn

    def registerConsumer(self, consumerId: UUID):
        self.logger.info(msg=f"Registering Consumer: {consumerId}")
        consumerConn, brokerConn = multiprocessing.Pipe()
        self.consumerMap[consumerId] = brokerConn

        consumerTaskConn, brokerTaskConn = multiprocessing.Pipe()
        self.consumerTaskMap[consumerId] = brokerTaskConn

        self.consumerWaitingQueue.put_nowait(consumerId)
        self.logger.info(msg=f"Consumer: {consumerId} Successfully Registered")
        self.logger.info(msg=f"Available Consumers {self.consumerTaskMap.keys()}")
        return consumerConn, consumerTaskConn
