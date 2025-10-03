# sts_parallel_scan_next

## Location
[src/backend/utils/sort/sharedtuplestore.c:495-597](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/sharedtuplestore.c#L495-L597)

## Overview
Retrieves the next tuple during a parallel scan of a shared tuple store, coordinating between multiple worker processes to efficiently distribute tuples across participants.

## Definition

```c
MinimalTuple
sts_parallel_scan_next(SharedTuplestoreAccessor *accessor, void *meta_data)
```
## Detailed Description
This function implements the core logic for parallel scanning of shared tuple stores. It manages the complex coordination between multiple worker processes that are reading from the same shared tuple store. The function operates by:

1. **Chunk-based Reading**: Attempts to read more tuples from the current chunk if available
2. **Participant Coordination**: Uses locks to coordinate access between multiple participants, ensuring each chunk is read by only one participant
3. **File Management**: Opens and manages temporary files for each participant as needed
4. **Overflow Handling**: Skips overflow chunks that contain continuation data from large tuples
5. **Round-robin Scanning**: When one participant's data is exhausted, moves to the next participant's file

The function uses a lock-based protocol where each participant has a  pointer that advances in  increments to claim the next chunk for processing. This ensures parallel workers don't duplicate work.

## Parameters / Member Variables
- `*accessor`: SharedTuplestoreAccessor containing the current scan state, participant information, and file handles
- `*meta_data`: Metadata buffer passed to tuple reading functions for additional tuple information
## Dependencies
- Functions called/Symbols referenced:
  - : Reads individual tuples from the current chunk
  - : Generates the filename for a participant's temporary file
  - : Opens a temporary file in the shared fileset
  - : Seeks to a specific block in the file
  - : Reads exact amount of data from file
  - : Closes temporary file handle
  - /: Provides exclusive access to participant state
- Called from (representative examples):
  - : During hash join repartitioning
  - : When fetching outer tuples for hash joins
  - : When starting a new batch in parallel hash joins

## Notes and Other Information
- Returns  when no more tuples are available from any participant
- The function handles file I/O errors and reports them using PostgreSQL's error reporting system
- Overflow chunks are automatically skipped - these contain continuation data for tuples that span multiple chunks
- Memory context switching ensures file handles are allocated in the correct context
- The round-robin participant scanning ensures load balancing across all participants in the shared tuple store
- This function is critical for parallel query execution, particularly in hash joins where tuple redistribution is necessary