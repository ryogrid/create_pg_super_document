# pq_redirect_to_shm_mq

## Location
[src/backend/libpq/pqmq.c:53-66](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqmq.c#L53-L66)

## Overview
Redirects frontend/backend protocol messages to a shared-memory message queue, enabling PostgreSQL parallel workers to communicate via shared memory instead of traditional sockets.

## Definition
```c
void pq_redirect_to_shm_mq(dsm_segment *seg, shm_mq_handle *mqh)
```

## Detailed Description
This function sets up message queue communication for parallel workers by redirecting PostgreSQL's standard libpq protocol messages to use shared memory message queues instead of the normal socket-based communication. It configures the communication infrastructure and registers a cleanup handler to ensure proper teardown when the dynamic shared memory segment is detached.

The function switches the communication method from the default socket-based approach to the shared memory message queue approach, which is essential for parallel query execution where workers need to send results back to the leader process efficiently.

## Parameters / Member Variables
- `seg`: Dynamic shared memory segment that will be used for communication
- `mqh`: Handle to the shared memory message queue for sending messages

## Dependencies
- Functions called/Symbols referenced:
  - [on_dsm_detach](../o/on_dsm_detach.md)
  - [pq_cleanup_redirect_to_shm_mq](pq_cleanup_redirect_to_shm_mq.md)
  - PqCommMqMethods
  - DestRemote
  - PG_PROTOCOL_LATEST
- Called from (representative examples):
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md)
  - [ParallelApplyWorkerMain](../P/ParallelApplyWorkerMain.md)

## Notes and Other Information
- Sets up the communication method pointer (PqCommMethods) to use message queue methods
- Configures output destination to DestRemote for proper message routing
- Sets the frontend protocol version to the latest supported version
- Registers a cleanup callback that will be invoked when the DSM segment is detached
- This is a critical setup function for parallel query execution infrastructure

## Simplified Source

```c
void pq_redirect_to_shm_mq(dsm_segment *seg, shm_mq_handle *mqh) {
    // Switch communication method to use shared memory message queues
    PqCommMethods = &PqCommMqMethods;

    // Store the message queue handle for later use
    pq_mq_handle = mqh;

    // Configure output destination for remote communication
    whereToSendOutput = DestRemote;

    // Set protocol version to latest
    FrontendProtocol = PG_PROTOCOL_LATEST;

    // Register cleanup function to run when DSM segment is detached
    on_dsm_detach(seg, pq_cleanup_redirect_to_shm_mq, (Datum) 0);
}
```