import asyncio
import logging
import multiprocessing
import queue
import time
from asyncio import CancelledError, Queue, QueueEmpty
from pathlib import Path
from typing import List
from uuid import UUID

import aiofiles
import orjson
from filelock import BaseAsyncFileLock

from singleJsonDistributedQueue.enum.EventType import EventType
from singleJsonDistributedQueue.model.Event import Event
from singleJsonDistributedQueue.model.Task import TaskIn


class PublisherBroker:
    @staticmethod
    def initPublisherBroker(
        brokerQueue: multiprocessing.Queue,
        acknowledgementQueue: multiprocessing.Queue,
        jsonQueueLock: BaseAsyncFileLock,
    ):
        publisherBroker = PublisherBroker(
            brokerQueue=brokerQueue, acknowledgementQueue=acknowledgementQueue, jsonQueueLock=jsonQueueLock
        )

        asyncio.run(publisherBroker.run())

    def __init__(
        self,
        brokerQueue: multiprocessing.Queue,
        acknowledgementQueue: multiprocessing.Queue,
        jsonQueueLock: BaseAsyncFileLock,
    ):
        self.brokerQueue = brokerQueue
        self.acknowledgementQueue = acknowledgementQueue
        self.writeQueue: Queue[TaskIn] = Queue()
        self.readQueue: Queue[TaskIn] = Queue()

        self.lastWriteQueueFlush = time.monotonic()
        self.lastReadQueueFlush = time.monotonic()

        self.jsonQueueLock = jsonQueueLock
        self.jsonQueuePath = Path(r"src\singleJsonDistributedQueue\queue\Queue.json")

        self.logger = logging.getLogger(name=__name__)
        self.logger.setLevel(logging.DEBUG)

        self.logger.info(msg="Publisher Broker Successfully Initialized")

    async def run(self):
        self.logger.info(msg="Publisher Broker Starting....")
        while True:
            try:
                currEvent: Event = await asyncio.to_thread(self.brokerQueue.get, True, 1)

                self.logger.info(
                    msg=f"Received A {currEvent.eventType} Event. Last Write: {time.monotonic() - self.lastWriteQueueFlush}"
                )

                if currEvent.eventType == EventType.SHUTDOWN:
                    await self.write(self.writeQueue)
                    await self.read(self.readQueue)
                    break
                elif currEvent.eventType == EventType.WRITE and currEvent.task is not None:
                    await self.writeQueue.put(item=currEvent.task)
                elif currEvent.eventType == EventType.READ and currEvent.task is not None:
                    await self.readQueue.put(item=currEvent.task)

                if time.monotonic() - self.lastWriteQueueFlush >= 2:
                    await self.write(taskDetails=self.writeQueue)
                    self.lastWriteQueueFlush = time.monotonic()

                if time.monotonic() - self.lastReadQueueFlush >= 2:
                    await self.read(taskDetails=self.readQueue)
                    self.lastReadQueueFlush = time.monotonic()

            except queue.Empty:
                pass
            except CancelledError:
                return

    async def write(self, taskDetails: Queue[TaskIn]):

        self.logger.info(msg="Publisher Broker About to Write Some Task into Queue")

        async with self.jsonQueueLock:
            try:
                async with aiofiles.open(file=self.jsonQueuePath, mode="r") as jsonQueueFile:
                    content = await jsonQueueFile.read()
                    if not content.strip():
                        data = {}
                    else:
                        data = orjson.loads(content)
            except FileNotFoundError:
                data = {}

            try:
                publisherIds: List[UUID] = []
                while True:
                    try:
                        taskDetail = taskDetails.get_nowait()
                    except QueueEmpty:
                        break
                    publisherIds.append(taskDetail.publisherId)
                    if taskDetail.taskId in data:
                        data[str(taskDetail.taskId)] = taskDetail.toTask()
                    else:
                        data[str(taskDetail.taskId)] = taskDetail.toTask()

                async with aiofiles.open(file=self.jsonQueuePath, mode="w") as jsonQueueFile:
                    jsonString = orjson.dumps(data, option=orjson.OPT_INDENT_2).decode()
                    await jsonQueueFile.write(jsonString)

                while publisherIds:
                    publisherId = publisherIds.pop()
                    await asyncio.to_thread(self.acknowledgeWrite, publisherId)

            except Exception:
                self.logger.exception("Something wrong occurred")
                raise

    async def read(self, taskDetails: Queue[TaskIn]):
        pass

    def acknowledgeWrite(self, publisherId: UUID):
        self.acknowledgementQueue.put_nowait(publisherId)
