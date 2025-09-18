# shm_mq_receive

## Location
src/backend/storage/ipc/shm_mq.c: 572 - 819

## Overview
Receives a complete message from a shared memory message queue, handling both contiguous and fragmented data efficiently.

## Definition
```c
shm_mq_result shm_mq_receive(shm_mq_handle *mqh, Size *nbytesp, void **datap, bool nowait)
```

## Detailed Description
This function implements the complete message reception protocol for shared memory queues. It reads length-prefixed messages and attempts to minimize data copying by returning direct pointers into shared memory when possible. For messages that wrap around the ring buffer or are received in fragments, it manages temporary buffers to reassemble complete messages.

Key operational features:
- **Zero-Copy Optimization**: When messages are available as contiguous blocks in shared memory, returns direct pointers without copying
- **Automatic Buffering**: For fragmented messages, transparently manages temporary buffers that grow as needed
- **Flow Control**: Implements receiver-side batching - only updates shared consumption counters when >1/4 of ring buffer is consumed
- **Robust State Management**: Handles partial reads across multiple calls, maintaining progress through complex state tracking
- **Sender Synchronization**: Waits for sender attachment and properly handles sender detachment scenarios

The function processes messages in phases: sender attachment verification, optional consumption updates, length word reading, and payload data reading with appropriate buffer management.

## Parameters / Member Variables
- `mqh`: Handle to the shared memory queue for receiving
- `nbytesp`: Output parameter set to the received message length in bytes
- `datap`: Output parameter set to point to message data (either direct shared memory or temporary buffer)
- `nowait`: If true, returns immediately when no data available instead of blocking

## Dependencies
- Functions called/Symbols referenced:
  - [shm_mq_receive_bytes](shm_mq_receive_bytes.md) (low-level data reading)
  - [shm_mq_inc_bytes_read](shm_mq_inc_bytes_read.md) (update consumption counters)
  - [shm_mq_counterparty_gone](shm_mq_counterparty_gone.md) (sender status checking)
  - [shm_mq_wait_internal](shm_mq_wait_internal.md) (blocking wait implementation)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (buffer allocation)
  - pg_nextpower2_size_t (buffer size calculation)
- Called from (representative examples):
  - [HandleParallelMessages](../H/HandleParallelMessages.md) (parallel query message processing)
  - [TupleQueueReaderNext](../T/TupleQueueReaderNext.md) (tuple queue operations)
  - [LogicalParallelApplyLoop](../L/LogicalParallelApplyLoop.md) (logical replication)

## Notes and Other Information
- Only the designated receiver process (mq->mq_receiver == MyProc) can call this function
- Returned data pointers remain valid until the next receive operation on the same queue
- Prevents memory exhaustion by limiting message sizes to MaxAllocSize
- Uses efficient power-of-2 buffer growth strategy for temporary buffers up to MaxAllocSize
- Implements careful race condition handling for sender attachment detection in nowait mode
- Returns SHM_MQ_SUCCESS with data, SHM_MQ_WOULD_BLOCK if nowait and no data available, or SHM_MQ_DETACHED if sender disconnected
- Automatically manages alignment requirements and consumption accounting for optimal ring buffer utilization