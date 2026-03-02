import asyncio
import logging
import multiprocessing
import queue
import time
from asyncio import CancelledError, Queue, QueueEmpty
from pathlib import Path
from typing import Dict, List
from uuid import UUID

import aiofiles
import orjson
from filelock import BaseAsyncFileLock

from singleJsonDistributedQueue.enum.EventOwner import EventOwner
from singleJsonDistributedQueue.enum.EventType import EventType
from singleJsonDistributedQueue.model.Event import Event
from singleJsonDistributedQueue.model.Task import Task


class ConsumerBroker:
    @staticmethod
    def initConsumerBroker(
        brokerQueue: multiprocessing.Queue,
        acknowledgementQueue: multiprocessing.Queue,
        consumerWaitingQueue: multiprocessing.Queue,
        consumerTaskMapQueue: multiprocessing.Queue,
        jsonQueueLock: BaseAsyncFileLock,
    ):
        consumerBroker = ConsumerBroker(
            brokerQueue=brokerQueue,
            acknowledgementQueue=acknowledgementQueue,
            jsonQueueLock=jsonQueueLock,
            consumerWaitingQueue=consumerWaitingQueue,
            consumerTaskMapQueue=consumerTaskMapQueue,
        )

        asyncio.run(consumerBroker.run())

    def __init__(
        self,
        brokerQueue: multiprocessing.Queue,
        acknowledgementQueue: multiprocessing.Queue,
        consumerWaitingQueue: multiprocessing.Queue,
        consumerTaskMapQueue: multiprocessing.Queue,
        jsonQueueLock: BaseAsyncFileLock,
    ):
        self.logger = logging.getLogger("ConsumerBroker")
        self.logger.setLevel(logging.DEBUG)

        self.logger.debug("Initializing Consumer Broker...")

        self.brokerQueue = brokerQueue

        self.acknowledgementQueue = acknowledgementQueue

        self.consumerWaitQueue = consumerWaitingQueue

        self.consumerTaskMapQueue = consumerTaskMapQueue

        self.writeQueue: Queue[Task] = Queue()
        self.readQueue: Queue[Task] = Queue()

        self.lastWriteQueueFlush = time.monotonic()
        self.lastReadQueueFlush = time.monotonic()

        self.jsonQueueLock = jsonQueueLock
        self.jsonQueuePath = Path(r"src\singleJsonDistributedQueue\queue\Queue.json")

        self.logger.info("Consumer Broker initialized.")

    async def run(self):
        self.logger.info("Starting event loop...")

        asyncio.create_task(self.assignTask())

        while True:
            try:
                currEvent: Event = await asyncio.to_thread(self.brokerQueue.get, True, 1)

                self.logger.debug(
                    f"Event: {currEvent.eventType} | Time since last write: {time.monotonic() - self.lastWriteQueueFlush:.2f}s"
                )

                if currEvent.eventType == EventType.SHUTDOWN:
                    await self.write(self.writeQueue)
                    await self.read(self.readQueue)
                    break
                elif currEvent.eventType == EventType.WRITE and isinstance(currEvent.task, Task):
                    await self.writeQueue.put(item=currEvent.task)
                elif currEvent.eventType == EventType.READ and isinstance(currEvent.task, Task):
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

    async def assignTask(self):
        self.logger.info("Task assignment loop started.")
        while True:
            await asyncio.sleep(2.0)
            try:
                try:
                    async with aiofiles.open(file=self.jsonQueuePath, mode="r") as jsonQueueFile:
                        content = await jsonQueueFile.read()
                        if not content.strip():
                            data: Dict[str, Dict] = {}
                        else:
                            data: Dict[str, Dict] = orjson.loads(content)
                except FileNotFoundError:
                    data: Dict[str, Dict] = {}

                if len(data) == 0:
                    continue
                for taskIdStr, taskDetailDict in data.items():
                    taskDetail = Task(**taskDetailDict)
                    if not taskDetail.isStart and taskDetail.consumerId is None:
                        try:
                            consumerId = await asyncio.to_thread(self.consumerWaitQueue.get, True, 1)
                        except queue.Empty:
                            continue
                        self.logger.info(
                            f"Assigning Task {taskDetail.taskId} to Consumer {str(consumerId)[:8]}."
                        )
                        taskDetail.consumerId = consumerId
                        await asyncio.to_thread(self.putTaskIntoQueue, consumerId, taskDetail)

            except CancelledError:
                return

    async def write(self, taskDetails: Queue[Task]):
        self.logger.debug(f"Entering write function with approx {taskDetails.qsize()} tasks waiting.")

        async with self.jsonQueueLock:
            self.logger.debug("Acquired JSON Queue Lock.")
            try:
                async with aiofiles.open(file=self.jsonQueuePath, mode="r") as jsonQueueFile:
                    content = await jsonQueueFile.read()
                    if not content.strip():
                        data = {}
                        self.logger.debug("Queue file is empty.")
                    else:
                        data = orjson.loads(content)
                        self.logger.debug(f"Loaded {len(data)} existing tasks from file.")
            except FileNotFoundError:
                self.logger.warning("Queue file not found. Creating new task list.")
                data = {}

            try:
                consumerIds: List[UUID] = []
                processed_count = 0
                while True:
                    try:
                        taskDetail = taskDetails.get_nowait()
                        processed_count += 1
                    except QueueEmpty:
                        break

                    if taskDetail.consumerId is not None:
                        consumerIds.append(taskDetail.consumerId)

                        if taskDetail.isComplete:
                            if str(taskDetail.taskId) in data:
                                del data[str(taskDetail.taskId)]
                                self.logger.info(
                                    f"Ref-Id: {taskDetail.taskId} | Task COMPLETED. Removed from queue."
                                )
                            else:
                                self.logger.warning(
                                    f"Ref-Id: {taskDetail.taskId} | Task marked complete but not found in queue data."
                                )
                        else:
                            data[str(taskDetail.taskId)] = taskDetail
                            self.logger.info(f"Ref-Id: {taskDetail.taskId} | Task UPDATED/ADDED in queue.")
                    else:
                        self.logger.warning(
                            f"Ref-Id: {taskDetail.taskId} | Task has no consumerId. Skipping."
                        )

                self.logger.debug(f"Processed {processed_count} tasks from internal queue.")

                if processed_count > 0:
                    async with aiofiles.open(file=self.jsonQueuePath, mode="w") as jsonQueueFile:
                        jsonString = orjson.dumps(data, option=orjson.OPT_INDENT_2).decode()
                        await jsonQueueFile.write(jsonString)
                    self.logger.debug("Successfully wrote updated task list to file.")
                else:
                    self.logger.debug("No tasks to write. File untouched.")

                ack_count = 0
                while consumerIds:
                    consumerId = consumerIds.pop()
                    await asyncio.to_thread(self.acknowledgeWrite, consumerId)
                    ack_count += 1
                self.logger.debug(f"Sent acknowledgements to {ack_count} consumers.")

            except Exception:
                self.logger.exception("ERROR: Exception during queue write operation.")
                raise
            finally:
                self.logger.debug("Released JSON Queue Lock.")

    async def read(self, taskDetails: Queue[Task]):
        pass

    def acknowledgeWrite(self, consumerId: UUID):
        self.acknowledgementQueue.put_nowait((consumerId, EventOwner.CONSUMER))

    def putTaskIntoQueue(self, consumerId: UUID, taskDetail: Task):
        self.consumerTaskMapQueue.put_nowait((consumerId, taskDetail))
