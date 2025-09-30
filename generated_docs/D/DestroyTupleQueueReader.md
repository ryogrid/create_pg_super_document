# DestroyTupleQueueReader

## Location
[src/backend/executor/tqueue.c:155-175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/tqueue.c#L155-L175)

## Overview
Destroys and deallocates a TupleQueueReader object, freeing the memory used by the reader structure.

## Definition
void DestroyTupleQueueReader(TupleQueueReader *reader)

## Detailed Description
This function performs cleanup of a TupleQueueReader by freeing the memory allocated for the reader structure itself. It is the counterpart to CreateTupleQueueReader and should be called when the reader is no longer needed. The function specifically does NOT clean up the underlying shared memory message queue (shm_mq_handle) - that responsibility belongs to the caller, as the queue may be shared among multiple readers or already detached by the time this function is called.

## Parameters / Member Variables
- `reader`: A pointer to the TupleQueueReader structure to be destroyed and deallocated

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (memory deallocation)
  - [TupleQueueReader](../T/TupleQueueReader.md) (struct type)
- Called from (representative examples):
  - [ExecParallelFinish](../E/ExecParallelFinish.md)

## Notes and Other Information
- The caller is responsible for managing the lifecycle of the underlying shm_mq_handle
- This function only frees the TupleQueueReader wrapper structure, not the shared memory queue itself
- Should be called for every TupleQueueReader created with CreateTupleQueueReader to prevent memory leaks
- The underlying queue may already be detached when this function is called, which is acceptable
- Part of PostgreSQL's parallel execution cleanup process

## Simplified Source

```c
void DestroyTupleQueueReader(TupleQueueReader *reader) {
    // Free the reader structure memory
    // Note: caller responsible for cleaning up underlying shm_mq
    pfree(reader);
}
```