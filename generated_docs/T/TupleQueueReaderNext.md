# TupleQueueReaderNext

## Location
src/backend/executor/tqueue.c: 176 - 210

## Overview
Fetches the next tuple from a TupleQueueReader, providing the main interface for reading tuples from shared memory message queues in PostgreSQL's parallel execution system.

## Definition
MinimalTuple TupleQueueReaderNext(TupleQueueReader *reader, bool nowait, bool *done)

## Detailed Description
This function attempts to read the next tuple from a shared memory message queue through a TupleQueueReader. It serves as the primary data retrieval mechanism for parallel query execution, allowing the main backend process to receive tuples from worker processes. The function handles various queue states including blocking/non-blocking modes, queue detachment, and partial message reading. The returned tuple points directly to shared memory or a private buffer and remains valid only until the next call to this function. The function can accumulate bytes from partially-read messages even when no complete tuple is available, making it beneficial to call with nowait=true for progressive message assembly.

## Parameters / Member Variables
- `reader`: Pointer to the TupleQueueReader from which to fetch the next tuple
- `nowait`: Boolean flag indicating whether to return immediately if no tuple is ready (true) or block waiting for data (false)
- `done`: Optional output parameter that is set to true when no more tuples will be available (queue detached), false otherwise

## Dependencies
- Functions called/Symbols referenced:
  - shm_mq_receive (shared memory queue receive operation)
  - MinimalTuple (tuple data structure)
  - shm_mq_result (result enumeration)
  - SHM_MQ_DETACHED, SHM_MQ_WOULD_BLOCK, SHM_MQ_SUCCESS (status constants)
- Called from (representative examples):
  - gather_readnext
  - gm_readnext_tuple

## Notes and Other Information
- Returns NULL when no tuples are available or when nowait=true and no tuple is ready
- The returned tuple pointer becomes invalid after the next call to TupleQueueReaderNext()
- Tuples are returned in shared memory or private buffer format - do not free the returned pointer
- Can handle partial message assembly across multiple calls, useful for large tuples
- Queue detachment is detected and communicated through the done parameter
- Essential component of PostgreSQL's Gather and GatherMerge execution nodes for parallel query processing
- The tuple length validation (Assert(tuple->t_len == nbytes)) ensures data integrity