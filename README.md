# Distributed Queue

A lightweight, file-based distributed queue system implemented in Python. This project demonstrates various distributed queue implementations, starting with a simple JSON-based persistence model.

## 🚧 Status: Work in Progress

- **Single JSON Queue**: Publisher implementation complete (Write with ACK). Consumer logic pending.
- **Other Implementations**: Planned.

## Implementations

### 1. Single JSON Distributed Queue (`src/singleJsonDistributedQueue`)

This implementation uses a local JSON file as the persistent queue storage, managed by a dedicated broker process.

#### Key Features

- **File-Based Persistence**: Uses `Queue.json` as the single source of truth.
- **Process Isolation**: The Broker runs in a separate `multiprocessing.Process` to decouple file I/O from the main application.
- **Event-Driven Architecture**: Communication between Publisher/Consumer and the Broker is handled via strict `Event` objects passed through `multiprocessing.Queue`.
- **Batch Processing**: The Broker batches write operations (flushing every 2 seconds or on shutdown) to optimize file I/O.
- **Reliable Acknowledgements**: Publishers receive explicit acknowledgements via `multiprocessing.Pipe` after data is successfully persisted to disk.
- **Concurrency Control**: Uses `filelock` to ensure safe access to the JSON file across processes.

#### Architecture

The system consists of three main components:

1.  **Publisher (`Publisher.py`)**:
    -   Generates a `TaskIn` object containing data.
    -   Sends a `WRITE` event to the Broker Manager.
    -   Waits for a synchronous acknowledgement via a dedicated pipe connection.

2.  **Broker Manager (`BrokerManager.py`)**:
    -   Orchestrates the lifecycle of the `PublisherBroker`.
    -   Maintains a registry of connected Publishers (`publisherMap`) and their pipe connections.
    -   Listens for completion events from the Broker and routes acknowledgements back to the correct Publisher.

3.  **Publisher Broker (`PublisherBroker.py`)**:
    -   The heavy lifter running in a background process.
    -   Consumes events from the central `brokerQueue`.
    -   Buffers tasks internally and performs batch writes to `queue/Queue.json`.
    -   Uses `aiofiles` and `orjson` for high-performance non-blocking I/O.

#### Data Models

-   **`Event`**: The envelope for all communication. Contains `EventType` (READ/WRITE/SHUTDOWN), `EventOwner`, and the payload.
-   **`TaskIn`**: The data transfer object sent by Publishers. Includes a unique `taskId` and `publisherId`.

## Dependencies

-   Python >= 3.12
-   `aiofiles`: Asynchronous file I/O.
-   `filelock`: Platform-independent file locking.
-   `orjson`: Fast JSON serialization/deserialization.

## Setup

1.  **Install Dependencies**:

    ```bash
    pip install -r requirements.txt
    # OR with pyproject.toml
    pip install .
    ```

2.  **Run the Application**:

    ```bash
    python src/singleJsonDistributedQueue/main.py
    ```

## Usage Example

### Initializing the System

```python
import asyncio
from pathlib import Path
from filelock import AsyncFileLock
from singleJsonDistributedQueue.broker.BrokerManager import BrokerManager
from singleJsonDistributedQueue.Publisher import Publisher
from singleJsonDistributedQueue.model.Task import TaskIn

async def main():
    # 1. Initialize the Lock
    queue_lock = AsyncFileLock(Path("src/singleJsonDistributedQueue/queue/Queue.json.lock"))
    
    # 2. Start the Broker Manager
    manager = BrokerManager(jsonQueueLock=queue_lock)
    await manager.run()
    
    # 3. Create a Publisher
    publisher = Publisher(brokerManager=manager)
    
    # 4. Send a Write Request
    new_task = TaskIn(data="Process this data", isStart=True)
    try:
        await publisher.writeRequest(new_task)
        print(f"Task {new_task.taskId} written successfully!")
    except TimeoutError:
        print("Failed to get acknowledgement.")
        
    # Cleanup
    await manager.close()

if __name__ == "__main__":
    asyncio.run(main())
```

## Project Structure

```text
src/singleJsonDistributedQueue/
├── broker/          
│   ├── BrokerManager.py     # Process orchestration & ACK routing
│   └── PublisherBroker.py   # File I/O & Batch processing logic
├── enum/            
│   ├── EventOwner.py        # Enums for system components
│   └── EventType.py         # WRITE, READ, SHUTDOWN
├── model/           
│   ├── Event.py             # Communication envelope
│   └── Task.py              # TaskIn (DTO) and Task (Storage) models
├── queue/           
│   └── Queue.json           # Data storage
├── Publisher.py     # Client interface for writing tasks
├── Consumer.py      # Client interface for reading tasks
└── main.py          
```
