import asyncio
import logging
import multiprocessing
from asyncio import CancelledError
from multiprocessing import Queue
from multiprocessing.connection import PipeConnection
from queue import Empty
from typing import Dict
from uuid import UUID

from filelock import BaseAsyncFileLock

from singleJsonDistributedQueue.broker.PublisherBroker import PublisherBroker
from singleJsonDistributedQueue.enum.EventOwner import EventOwner
from singleJsonDistributedQueue.enum.EventType import EventType
from singleJsonDistributedQueue.model.Event import Event


class BrokerManager:
    def __init__(self, jsonQueueLock: BaseAsyncFileLock):
        self.brokerQueue: Queue[Event] = multiprocessing.Queue()
        self.acknowledgementQueue: Queue[UUID] = multiprocessing.Queue()

        self.publisherMap: Dict[UUID, PipeConnection] = {}

        self.publisherBrokerProcess = multiprocessing.Process(
            target=PublisherBroker.initPublisherBroker,
            args=(self.brokerQueue, self.acknowledgementQueue, jsonQueueLock),
        )

        self.ackTask: asyncio.Task | None = None

        self.logger = logging.getLogger(name=__name__)
        self.logger.setLevel(logging.DEBUG)

        self.logger.info(msg="Broker Manager Successfully Initialized")

    async def run(self):
        self.logger.info(msg="Broker Manager Starting....")

        self.publisherBrokerProcess.start()
        self.ackTask = asyncio.create_task(self.listenAcknowledgement())

    async def close(self):
        self.logger.info(msg="Broker Manager Closing....")

        await self.shutdownBroker()

        if self.ackTask is not None:
            self.ackTask.cancel()
            await self.ackTask

        self.logger.info(msg="Broker Manager Successfully Closed")

    async def listenAcknowledgement(self):
        self.logger.info(msg="Broker Manager Started to listen acknowledgements....")

        while True:
            try:
                currAck: UUID = await asyncio.to_thread(self.acknowledgementQueue.get, True, 1)
                conn = self.publisherMap.get(currAck)
                if conn is not None:
                    conn.send(True)
                else:
                    self.logger.warning(f"ACK received for unknown publisher {currAck}")
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
                self.brokerQueue.put,
                Event(eventType=EventType.SHUTDOWN, eventOwner=EventOwner.BROKER_MANAGER),
            )

        await asyncio.to_thread(self.publisherBrokerProcess.join)

        self.logger.info(msg="Publisher Broker Successfully Shutdown")

    def registerPublisher(self, publisherId: UUID):
        publisherConn, brokerConn = multiprocessing.Pipe()
        self.publisherMap[publisherId] = brokerConn
        return publisherConn
