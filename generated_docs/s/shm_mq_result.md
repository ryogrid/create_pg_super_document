# shm_mq_result

## Location
[src/include/storage/shm_mq.h:41-86](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/shm_mq.h#L41-L86)

## Overview
An enumerated type that represents the possible results of shared memory message queue send or receive operations in PostgreSQL's inter-process communication system.

## Definition

```c
typedef enum
{
	SHM_MQ_SUCCESS,				/* Sent or received a message. */
	SHM_MQ_WOULD_BLOCK,			/* Not completed; retry later. */
	SHM_MQ_DETACHED,			/* Other process has detached queue. */
} shm_mq_result;
```
## Detailed Description
The `shm_mq_result` enum defines the possible outcomes when performing message queue operations in PostgreSQL's shared memory message queue (shm_mq) system. This system provides a high-performance, lock-free communication mechanism between processes, particularly used in parallel query execution and background worker communication.

The enum serves as a status indicator for all major shm_mq operations, allowing callers to understand whether their operation succeeded, needs to be retried, or failed due to the counterpart process disconnecting. This design supports both blocking and non-blocking operation modes, with the enum values providing the necessary feedback for implementing proper retry logic and error handling.

## Parameters / Member Variables
- `SHM_MQ_SUCCESS`: The operation completed successfully. For send operations, this means the message was fully written to the queue. For receive operations, this means a complete message was received and is available to the caller.
- `SHM_MQ_WOULD_BLOCK`: The operation could not complete immediately and would need to block to succeed. This is returned in non-blocking mode when the queue is full (for sends) or empty (for receives). The caller should retry the operation later, typically after waiting for the process latch to be set.
- `SHM_MQ_DETACHED`: The counterpart process has detached from the queue, making further communication impossible. This typically occurs when a background worker process terminates or when a parallel worker encounters a fatal error.

## Dependencies
- Functions that return this type:
  - `shm_mq_send`: Send a message to the queue
  - `shm_mq_sendv`: Send a vectored message to the queue  
  - `shm_mq_receive`: Receive a message from the queue
  - `shm_mq_wait_for_attach`: Wait for counterparty to attach
- Used extensively by callers (representative examples):
  - `TupleQueueReaderNext`: Reading tuples in parallel execution
  - `HandleParallelMessages`: Processing parallel worker messages
  - `mq_putmessage`: libpq message queue operations
  - `LogicalParallelApplyLoop`: Logical replication parallel apply
  - `pa_send_data`: Parallel apply worker data transmission

## Notes and Other Information
- The enum is designed to support both blocking and non-blocking operation modes, with `SHM_MQ_WOULD_BLOCK` being particularly important for non-blocking operations
- When `SHM_MQ_WOULD_BLOCK` is returned, the partial operation state is maintained, and the exact same call should be retried later with the same parameters
- `SHM_MQ_DETACHED` is considered a permanent failure condition - operations should not be retried after receiving this result
- The enum is frequently used in loops that retry operations until either success or permanent failure
- This return code system enables efficient implementation of timeout mechanisms and responsive cancellation handling in parallel processing scenarios
- The shared memory message queue system using this enum is lock-free and designed for high-performance inter-process communication in PostgreSQL's parallel execution framework