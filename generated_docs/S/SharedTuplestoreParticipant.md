# SharedTuplestoreParticipant

## Location
[src/backend/utils/sort/sharedtuplestore.c:50-56](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/sharedtuplestore.c#L50-L56)

## Overview
SharedTuplestoreParticipant represents the per-participant shared state in PostgreSQL shared tuple store system, managing individual participant access to the shared tuple data with synchronization and page tracking.

## Definition
```c
typedef struct SharedTuplestoreParticipant
{
    LWLock      lock;
    BlockNumber read_page;      /* Page number for next read. */
    BlockNumber npages;         /* Number of pages written. */
    bool        writing;        /* Used only for assertions. */
} SharedTuplestoreParticipant;
```

## Detailed Description
SharedTuplestoreParticipant maintains the state for each participant in a shared tuple store operation. Each participant has its own instance of this structure to track its current position in reading tuple data, the total pages it has written, and synchronization mechanisms. The structure ensures thread-safe access through lightweight locks and provides position tracking for efficient sequential scanning of tuple data. This design enables multiple processes to participate in parallel tuple store operations while maintaining consistency and avoiding conflicts.

## Parameters / Member Variables
- `lock`: LWLock for synchronizing access to this participant shared state between processes
- `read_page`: The page number where this participant will perform its next read operation 
- `npages`: Total number of pages that this participant has written to the shared tuple store
- `writing`: Boolean flag used for debugging assertions to track whether this participant is currently in write mode

## Dependencies
- Functions called/Symbols referenced:
  - LWLock (lightweight lock structure for inter-process synchronization)

- Called from (representative examples):
  - SharedTuplestore (main structure that contains arrays of participants)
  - sts_estimate (function that estimates memory requirements based on participant count)
  - sts_puttuple (function that uses participant state during tuple writing)
  - sts_parallel_scan_next (function that manages participant state during parallel scanning)

## Notes and Other Information
- Each participant in a shared tuple store operation has its own instance of this structure
- The lock ensures thread-safe access to participant state across multiple processes
- The read_page tracking enables efficient sequential access patterns during tuple retrieval
- The writing flag is primarily used for debugging and assertions to catch improper usage patterns
- This structure is essential for coordinating parallel operations in PostgreSQL query execution