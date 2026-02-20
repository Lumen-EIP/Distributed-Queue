import asyncio
import logging
from pathlib import Path
from random import randint

from filelock import AsyncFileLock

from singleJsonDistributedQueue.broker.BrokerManager import BrokerManager
from singleJsonDistributedQueue.model.Task import TaskIn
from singleJsonDistributedQueue.Publisher import Publisher

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")

logger = logging.getLogger(name=__name__)
logger.setLevel(logging.DEBUG)


async def subMain():
    logger.info(msg="subMain function about to Start....")

    logger.info(msg="Creating AsyncFileLock for Queue.json....")
    globalQueueLock = AsyncFileLock(Path(r"src\singleJsonDistributedQueue\queue\Queue.json.lock"))
    logger.info(msg="AsyncFileLock for Queue.json Successfully Created")

    logger.info(msg="Creating globalBrokerManager....")
    globalBrokerManager = BrokerManager(jsonQueueLock=globalQueueLock)
    logger.info(msg="globalBrokerManager Successfully Created")
    logger.info(msg="globalBrokerManager About to run....")
    await globalBrokerManager.run()

    logger.info(msg="Creating Publishers....")
    publisherPool = [Publisher(brokerManager=globalBrokerManager) for _ in range(10)]
    logger.info(msg="Publishers Successfully Created")

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
    except Exception:
        for task in publisherTasks:
            task.cancel()
    finally:
        await globalBrokerManager.close()


def main():
    logger.info(msg="Main function running....")
    asyncio.run(subMain())


if __name__ == "__main__":
    main()
