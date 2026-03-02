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
        self.logger = logging.getLogger("BrokerManager")
        self.logger.setLevel(logging.DEBUG)

        self.logger.debug("Initializing Broker Manager...")

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

        self.logger.info("Broker Manager initialized.")

    async def run(self):
        self.logger.info("Starting broker processes...")

        self.publisherBrokerProcess.start()
        self.consumerBrokerProcess.start()
        self.ackTask = asyncio.create_task(self.listenAcknowledgement())
        self.assignTask = asyncio.create_task(self.listenTaskAssignment())

    async def close(self):
        self.logger.info("Broker Manager shutting down...")

        await self.shutdownBroker()

        if self.ackTask is not None:
            self.ackTask.cancel()
            await self.ackTask

        if self.assignTask is not None:
            self.assignTask.cancel()
            await self.assignTask

        self.logger.info("Broker Manager closed.")

    async def listenAcknowledgement(self):
        self.logger.debug("Listening for acknowledgements...")

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
                    self.logger.warning(f"ACK received for unknown ID: {receiverId}")
            except Empty:
                pass
            except CancelledError:
                break

    async def listenTaskAssignment(self):
        self.logger.debug("Listening for task assignments...")

        while True:
            try:
                (consumerId, assignedTask) = await asyncio.to_thread(self.consumerTaskMapQueue.get, True, 1)
                conn = self.consumerTaskMap[consumerId]
                if conn is not None:
                    conn.send(assignedTask)
                    self.logger.info(f"Task dispatched to Consumer {str(consumerId)[:8]}.")
                else:
                    self.logger.warning(f"Task assignment for unknown Consumer {str(consumerId)[:8]}.")
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
        self.logger.info("Stopping Publisher Broker...")
        if self.publisherBrokerProcess and self.publisherBrokerProcess.is_alive():
            await asyncio.to_thread(
                self.publisherBrokerQueue.put,
                Event(eventType=EventType.SHUTDOWN, eventOwner=EventOwner.BROKER_MANAGER),
            )

        self.logger.info("Stopping Consumer Broker...")
        if self.consumerBrokerProcess and self.consumerBrokerProcess.is_alive():
            await asyncio.to_thread(
                self.consumerBrokerQueue.put,
                Event(eventType=EventType.SHUTDOWN, eventOwner=EventOwner.BROKER_MANAGER),
            )

        if self.publisherBrokerProcess:
            await asyncio.to_thread(self.publisherBrokerProcess.join, 5)

            if self.publisherBrokerProcess.is_alive():
                self.logger.warning("Publisher Broker hung, forcing termination.")
                self.publisherBrokerProcess.terminate()
                await asyncio.to_thread(self.publisherBrokerProcess.join)
        self.logger.info("Publisher Broker stopped.")

        if self.consumerBrokerProcess:
            await asyncio.to_thread(self.consumerBrokerProcess.join, 5)

            if self.consumerBrokerProcess.is_alive():
                self.logger.warning("Consumer Broker hung, forcing termination.")
                self.consumerBrokerProcess.terminate()
                await asyncio.to_thread(self.consumerBrokerProcess.join)
        self.logger.info("Consumer Broker stopped.")

    def registerPublisher(self, publisherId: UUID):
        publisherConn, brokerConn = multiprocessing.Pipe()
        self.publisherMap[publisherId] = brokerConn
        return publisherConn

    def registerConsumer(self, consumerId: UUID):
        self.logger.info(f"Registering Consumer {str(consumerId)[:8]}...")
        consumerConn, brokerConn = multiprocessing.Pipe()
        self.consumerMap[consumerId] = brokerConn

        consumerTaskConn, brokerTaskConn = multiprocessing.Pipe()
        self.consumerTaskMap[consumerId] = brokerTaskConn

        self.consumerWaitingQueue.put_nowait(consumerId)
        self.logger.info(f"Consumer {str(consumerId)[:8]} registered.")
        return consumerConn, consumerTaskConn
