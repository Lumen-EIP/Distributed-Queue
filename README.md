# Distributed Queue

A lightweight, file-based distributed queue system implemented in Python. This project demonstrates various distributed queue implementations, starting with a simple JSON-based persistence model.

## 🚧 Status: Work in Progress

- **Single JSON Queue**: Full Publisher-Consumer loop implemented.
    - Publishers write tasks with ACK.
    - Consumers register, receive tasks, process them, and update status.
    - Broker splits logic into separate Writer (Publisher) and Reader (Consumer) processes.
- **Other Implementations**: Planned.

## Implementations

### 1. Single JSON Distributed Queue (`src/singleJsonDistributedQueue`)

This implementation uses a local JSON file as the persistent queue storage, managed by a dedicated broker process.

#### Key Features

- **File-Based Persistence**: Uses `Queue.json` as the single source of truth.
- **Dual-Broker Architecture**:
    - **PublisherBroker**: Dedicated process for handling high-throughput write operations.
    - **ConsumerBroker**: Dedicated process for task scheduling and assignment to idle consumers.
- **Process Isolation**: Brokers run in separate `multiprocessing.Process` instances to decouple file I/O and logic from the main application.
- **Event-Driven Architecture**: Communication uses strict `Event` objects passed through `multiprocessing.Queue`.
- **Consumer Lifecycle Management**:
    - Consumers explicitly register with the Broker.
    - Idle consumers are tracked in a `WaitingQueue`.
    - Tasks are pushed to consumers via dedicated `Pipe` connections (Push model).
- **Reliable Acknowledgements**: Both Publishers and Consumers receive explicit acknowledgements for their operations.
- **Concurrency Control**: Uses `filelock` to ensure safe access to the JSON file across multiple processes.

#### Architecture

The system consists of three main components:

1.  **Publisher (`Publisher.py`)**:
    -   Generates a `TaskIn` object containing data.
    -   Sends a `WRITE` event to the `PublisherBroker`.
    -   Waits for synchronous acknowledgement via a dedicated pipe connection.

2.  **Consumer (`Consumer.py`)**:
    -   Registers with the `BrokerManager` to receive a dedicated Task Pipe.
    -   Enters a wait loop listening for assigned tasks.
    -   Processes tasks (simulates work) and sends updates back to the system.
    -   Automatically re-queues itself as "Available" after task completion.

3.  **Broker Manager (`BrokerManager.py`)**:
    -   **Orchestrator**: Manages the lifecycle of `PublisherBroker` and `ConsumerBroker` processes.
    -   **Registry**: Maintains maps of all active Publishers and Consumers.
    -   **Router**: Listens for completion events and routes acknowledgements/tasks to the correct specific process via Pipes.

4.  **Broker Processes**:
    -   **PublisherBroker**: Batches write operations to `Queue.json` (flushing every 2s).
    -   **ConsumerBroker**: Monitors the queue for pending tasks and assigns them to available consumers from the `WaitingQueue`.

#### Data Models

-   **`Event`**: The envelope for communication. Contains `EventType`, `EventOwner`, and payload.
-   **`TaskIn`**: DTO sent by Publishers.
-   **`Task`**: Full task object stored in `Queue.json` and processed by Consumers.

## Dependencies

-   Python >= 3.12
-   `aiofiles`: Asynchronous file I/O.
-   `filelock`: Platform-independent file locking.
-   `orjson`: Fast JSON serialization/deserialization.
-   `uv`: (Optional) For fast project management and running.

## Setup

1.  **Install Dependencies**:

    ```bash
    pip install -r requirements.txt
    # OR with pyproject.toml
    pip install .
    ```

    *Alternatively, if using `uv`:*
    ```bash
    uv sync
    ```

2.  **Run the Single JSON Queue Application**:

    The `main.py` entry point initializes the Broker Manager, spins up a pool of Publishers (10) and Consumers (5), and simulates a workload.

    ```bash
    python -m singleJsonDistributedQueue.main
    ```
    
    *Or with `uv`:*
    ```bash
    uv run python -m singleJsonDistributedQueue.main
    ```

    **Note**: Run this from the `src` directory or ensure `src` is in your `PYTHONPATH`.

## Project Structure

```text
src/singleJsonDistributedQueue/
├── broker/          
│   ├── BrokerManager.py     # Orchestrator, connection registry, routing logic
│   ├── PublisherBroker.py   # Handling Writes & Batching
│   └── ConsumerBroker.py    # Handling Task Assignment & Reads
├── enum/            
│   ├── EventOwner.py        # Enums for system components
│   └── EventType.py         # WRITE, READ, SHUTDOWN
├── model/           
│   ├── Event.py             # Communication envelope
│   └── Task.py              # data models
├── queue/           
│   └── Queue.json           # Data storage
├── Publisher.py     # Client interface for writing tasks
├── Consumer.py      # Client interface for processing tasks
└── main.py          # Demonstration entry point
```
