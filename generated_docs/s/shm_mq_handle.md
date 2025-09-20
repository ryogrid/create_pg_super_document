# shm_mq_handle

## Location
[src/backend/storage/ipc/shm_mq.c:137-170](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shm_mq.c#L137-L170)

## Overview
A backend-private handle structure that provides process-local access and state management for shared memory message queues (). It encapsulates the queue pointer, buffering, and operational state needed for efficient non-blocking I/O operations.

## Definition

```c
struct shm_mq_handle
{
	shm_mq	   *mqh_queue;
	dsm_segment *mqh_segment;
	BackgroundWorkerHandle *mqh_handle;
	char	   *mqh_buffer;
	Size		mqh_buflen;
	Size		mqh_consume_pending;
	Size		mqh_send_pending;
	Size		mqh_partial_bytes;
	Size		mqh_expected_bytes;
	bool		mqh_length_word_complete;
	bool		mqh_counterparty_attached;
	MemoryContext mqh_context;
};
```
## Detailed Description
The  serves as a process-local wrapper around a shared memory queue, providing essential functionality for both blocking and non-blocking operations:

- **Queue Access**: Maintains a pointer to the actual shared memory queue structure
- **DSM Integration**: Optional integration with dynamic shared memory segments, including automatic cleanup callbacks
- **Background Worker Integration**: Can track background worker handles to detect process failures early
- **Message Buffering**: Handles message reassembly for large messages or those that wrap around the ring buffer
- **Batched Updates**: Implements batched updates to reduce CPU cache misses and expensive SetLatch() calls
- **Non-blocking State**: Maintains state for resumable non-blocking operations that return SHM_MQ_WOULD_BLOCK

The handle optimizes performance by batching shared memory updates (writing data when it reaches 1/4 of ring size) and providing local buffering for fragmented messages.

## Parameters / Member Variables
- : Pointer to the shared memory queue structure this handle manages
- : Optional pointer to DSM segment containing the queue (enables cleanup callbacks)
- : BackgroundWorkerHandle for early failure detection in simple scenarios
- : Local buffer for reassembling wrapped or large messages
- : Size of the local reassembly buffer in bytes
- : Number of bytes read locally but not yet marked as consumed in shared memory
- : Number of bytes written locally but not yet updated in shared memory
- : Tracks partial progress in non-blocking operations (length word or message data)
- : Expected total message size for reads (used only for receive operations)
- : Flag indicating if length word processing is complete in current operation
- : Cached flag to avoid mutex acquisitions when checking counterparty attachment
- : Memory context for all allocations related to this handle

## Dependencies
- Functions called/Symbols referenced:
  - [shm_mq](shm_mq.md)
  - dsm_segment
  - [BackgroundWorkerHandle](../B/BackgroundWorkerHandle.md)
  - [MemoryContext](../M/MemoryContext.md)
- Called from (representative examples):
  - [shm_mq_attach](shm_mq_attach.md)
  - [shm_mq_send](shm_mq_send.md)
  - [shm_mq_receive](shm_mq_receive.md)
  - [CreateTupleQueueDestReceiver](../C/CreateTupleQueueDestReceiver.md)
  - [CreateTupleQueueReader](../C/CreateTupleQueueReader.md)
  - [ExecParallelSetupTupleQueues](../E/ExecParallelSetupTupleQueues.md)

## Notes and Other Information
- Optimizes performance through batched shared memory updates to minimize cache misses
- Supports both blocking and non-blocking I/O operations with resumable state
- Local message buffering handles ring buffer wrap-around and oversized messages transparently  
- Integration with DSM ensures proper cleanup when dynamic shared memory segments are detached
- Background worker handle integration provides early failure detection for simple communication patterns
- Memory context tracking ensures all allocations are properly managed and cleaned up
- Critical component in PostgreSQL's parallel execution framework, tuple queues, and logical replication