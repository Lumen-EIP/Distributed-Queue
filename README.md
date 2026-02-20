# Distributed Queue

A lightweight, file-based distributed queue system implemented in Python. This project demonstrates a producer-consumer implementation using a local JSON file as the persistent queue storage, managed by a broker process with file locking for concurrency safety.

## 🚧 Status: Work in Progress

The project is currently in the scaffolding phase. The core broker logic and data models are defined, but the implementation of full read/write operations and consumer logic is pending.

## Features

- **File-Based Persistence**: Uses `Queue.json` for persistent storage of tasks.
- **Concurrency Control**: Implements `filelock` and `aiofiles` for safe asynchronous file access.
- **Broker Pattern**:
  - `BrokerManager`: Manages the lifecycle of the queue broker process.
  - `PublisherBroker`: Handles low-level file I/O and task processing.
- **Event-Driven Architecture**: Uses an internal `multiprocessing.Queue` to pass events between the Application and the Broker.

## Architecture

The system consists of three main components:
1. **Publisher/Consumer**: (Stubs) Interfaces for client applications to push and pop tasks.
2. **Broker Manager**: Ensures the Broker process is running and healthy.
3. **Queue.json**: The single source of truth for the distributed queue state.

## Dependencies

- Python >= 3.12
- `aiofiles`: Asynchronous file I/O.
- `filelock`: Platform-independent file locking.
- `orjson`: Fast JSON serialization/deserialization.

## Setup

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   # OR with pyproject.toml
   pip install .
   ```

2. **Run the Application**:
   Currently, `main.py` initializes the Broker Manager.
   ```bash
   python src/singleJsonDistributedQueue/main.py
   ```

## Usage

*Current functionality is limited to initialization.*

```python
from pathlib import Path
from filelock import AsyncFileLock
from singleJsonDistributedQueue.broker.BrokerManager import BrokerManager

async def run_broker():
    # Initialize the lock file
    queue_lock = AsyncFileLock(Path("queue.lock"))
    
    # Start the manager
    manager = BrokerManager(jsonQueueLock=queue_lock)
    manager.run()
```

## Project Structure

```
src/singleJsonDistributedQueue/
├── broker/          # BrokerManager and PublisherBroker logic
├── enum/            # Event types and ownership definitions
├── model/           # Data classes for Events and Tasks
├── queue/           # Storage location for Queue.json
├── Publisher.py     # Publisher interface
├── Consumer.py      # Consumer interface
└── main.py          # Entry point
```
