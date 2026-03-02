import asyncio
import logging
from pathlib import Path
from random import randint

from filelock import AsyncFileLock

from singleJsonDistributedQueue.broker.BrokerManager import BrokerManager
from singleJsonDistributedQueue.Consumer import Consumer
from singleJsonDistributedQueue.model.Task import TaskIn
from singleJsonDistributedQueue.Publisher import Publisher

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(name="Main")
logger.setLevel(logging.DEBUG)


async def subMain():
    logger.info("Application starting...")

    logger.debug("Initializing file lock for Queue.json...")
    globalQueueLock = AsyncFileLock(Path(r"src\singleJsonDistributedQueue\queue\Queue.json.lock"))
    logger.debug("File lock for Queue.json initialized.")

    logger.info("Initializing Global Broker Manager...")
    globalBrokerManager = BrokerManager(jsonQueueLock=globalQueueLock)
    logger.info("Global Broker Manager initialized.")
    logger.info("Starting Global Broker Manager...")
    await globalBrokerManager.run()

    logger.info("Creating 10 Publisher instances...")
    publisherPool = [Publisher(brokerManager=globalBrokerManager) for _ in range(10)]
    logger.info("Publisher pool created.")

    logger.info("Creating 5 Consumer instances...")
    consumerPool = [Consumer(brokerManager=globalBrokerManager) for _ in range(5)]
    logger.info("Consumer pool created.")

    await asyncio.sleep(5)

    publisherTasks = []
    try:
        for publisher in publisherPool:
            publisherTasks.append(
                asyncio.create_task(
                    publisher.writeRequest(
                        taskDetail=TaskIn(
                            publisherId=publisher.publisherId, data="Hi", reqTime=randint(1, 20)
                        )
                    )
                )
            )

            await asyncio.sleep(0.5)
        await asyncio.gather(*publisherTasks, return_exceptions=True)
        await asyncio.sleep(10)
    except Exception:
        for task in publisherTasks:
            task.cancel()
    finally:
        await globalBrokerManager.close()
        for consumer in consumerPool:
            await consumer.close()


def main():
    logger.info("Main function running...")
    asyncio.run(subMain())


if __name__ == "__main__":
    main()
