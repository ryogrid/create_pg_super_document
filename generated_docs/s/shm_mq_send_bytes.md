# shm_mq_send_bytes

## Location
[src/backend/storage/ipc/shm_mq.c:914-1078](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shm_mq.c#L914-L1078)

## Overview
Writes bytes into a shared memory message queue, handling flow control, synchronization, and efficient data transfer between processes.

## Definition
```c
static shm_mq_result shm_mq_send_bytes(shm_mq_handle *mqh, Size nbytes, const void *data, bool nowait, Size *bytes_written)
```

## Detailed Description
This function implements the core message sending mechanism for PostgreSQL's shared memory message queues. It handles complex flow control by managing ring buffer space, waiting for receiver attachment, coordinating with the receiver process, and performing efficient memory copying with proper synchronization barriers.

The function operates in a loop, attempting to send data in chunks based on available ring buffer space. It uses atomic operations to read queue state, implements proper memory barriers for cache coherency, and coordinates with the receiver through process latches. The function supports both blocking and non-blocking operation modes and handles various edge cases including queue detachment and receiver unavailability.

Key optimizations include batching of pending send operations to reduce shared memory updates and latch operations, proper alignment handling for efficient memory access, and careful synchronization to prevent race conditions between sender and receiver processes.

## Parameters / Member Variables
- `mqh`: Handle to the shared memory message queue for sending data
- `nbytes`: Number of bytes to send from the data buffer
- `data`: Pointer to the data buffer containing bytes to send
- `nowait`: If true, function returns immediately when blocking would be required instead of waiting
- `bytes_written`: Output parameter that receives the actual number of bytes successfully written

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_read_u64](../p/pg_atomic_read_u64.md)
  - pg_compiler_barrier
  - pg_memory_barrier
  - [shm_mq_counterparty_gone](shm_mq_counterparty_gone.md)
  - [shm_mq_get_receiver](shm_mq_get_receiver.md)
  - [shm_mq_wait_internal](shm_mq_wait_internal.md)
  - [shm_mq_inc_bytes_written](shm_mq_inc_bytes_written.md)
  - [SetLatch](../S/SetLatch.md)
  - [WaitLatch](../W/WaitLatch.md)
  - [ResetLatch](../R/ResetLatch.md)
  - CHECK_FOR_INTERRUPTS
  - memcpy
- Called from (representative examples):
  - [shm_mq_sendv](shm_mq_sendv.md)

## Notes and Other Information
- Uses atomic operations and memory barriers to ensure proper synchronization between processes
- Implements flow control by checking available ring buffer space before writing
- Optimizes performance by batching pending send operations to reduce shared memory overhead
- Supports both blocking and non-blocking modes for different use cases
- Handles receiver attachment synchronization to prevent data loss
- Returns SHM_MQ_SUCCESS on complete success, SHM_MQ_WOULD_BLOCK for non-blocking partial sends, or SHM_MQ_DETACHED if queue is detached
- Critical component of PostgreSQL's parallel processing and inter-process communication infrastructure
- Implements proper MAXALIGN alignment for efficient memory access patterns