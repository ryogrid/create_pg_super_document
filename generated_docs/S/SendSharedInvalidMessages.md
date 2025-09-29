# SendSharedInvalidMessages

## Location
[src/backend/storage/ipc/sinval.c:48-69](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/sinval.c#L48-L69)

## Overview
SendSharedInvalidMessages adds shared-cache-invalidation message(s) to the global shared invalidation (SI) message queue for cross-backend cache invalidation.

## Definition
```c
void SendSharedInvalidMessages(const SharedInvalidationMessage *msgs, int n)
```

## Detailed Description
SendSharedInvalidMessages is a wrapper function that provides the interface for adding invalidation messages to PostgreSQL's shared invalidation system. When database objects (like tables, indexes, or cached data) are modified, invalidation messages must be sent to notify other backend processes that their cached copies of the data are no longer valid. This function takes an array of SharedInvalidationMessage structures and forwards them to the lower-level SIInsertDataEntries function, which handles the actual insertion into the global message queue.

The function serves as a clean API boundary between the cache invalidation logic and the shared memory management implementation, allowing the invalidation system to operate without directly manipulating the underlying shared invalidation data structures.

## Parameters / Member Variables
- `msgs`: Pointer to an array of SharedInvalidationMessage structures containing the invalidation messages to be sent
- `n`: Number of messages in the msgs array

## Dependencies
- Functions called/Symbols referenced:
  - [SIInsertDataEntries](SIInsertDataEntries.md) (handles actual insertion into shared invalidation queue)
  - [SharedInvalidationMessage](SharedInvalidationMessage.md) (message structure type)
- Called from (representative examples):
  - [FinishPreparedTransaction](../F/FinishPreparedTransaction.md) (during two-phase commit completion)
  - [ProcessCommittedInvalidationMessages](../P/ProcessCommittedInvalidationMessages.md) (when processing committed invalidations)  
  - [AtEOXact_Inval](../A/AtEOXact_Inval.md) (at end-of-transaction invalidation processing)
  - [CacheInvalidateSmgr](../C/CacheInvalidateSmgr.md) (for storage manager cache invalidation)
  - [CacheInvalidateRelmap](../C/CacheInvalidateRelmap.md) (for relation mapping cache invalidation)

## Notes and Other Information
- This function is part of PostgreSQL's shared invalidation system that ensures cache consistency across multiple backend processes
- The function is a thin wrapper that provides a clean interface while delegating the complex shared memory operations to SIInsertDataEntries
- Invalidation messages are critical for maintaining data consistency in a multi-process database system where each backend maintains its own caches
- The function is declared in src/include/storage/sinval.h and implemented in src/backend/storage/ipc/sinval.c

## Simplified Source

```c
void
SendSharedInvalidMessages(const SharedInvalidationMessage *msgs, int n)
{
    SIInsertDataEntries(msgs, n);
}
```