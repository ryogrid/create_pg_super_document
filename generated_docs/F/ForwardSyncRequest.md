# ForwardSyncRequest

## Location
[src/backend/postmaster/checkpointer.c:1099-1159](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/checkpointer.c#L1099-L1159)

## Overview
ForwardSyncRequest forwards a file-fsync request from a backend process to the checkpointer process, ensuring that dirty relation files are properly synchronized before the next checkpoint.

## Definition
bool ForwardSyncRequest(const FileTag *ftag, SyncRequestType type)

## Detailed Description
This function serves as a communication mechanism between backend processes and the checkpointer process for managing file synchronization requests. When a backend is compelled to write directly to a relation (which should be infrequent if the background writer is functioning properly), it calls this function to notify the checkpointer that the relation is dirty and must be fsync'd before the next checkpoint.

The function implements an optimization strategy where it normally writes to the requests queue without checking for duplicates, allowing the checkpointer to handle deduplication internally. However, if the queue becomes full, it performs a compaction pass to eliminate duplicates. This approach balances performance with queue management, as the alternative would be for backends to perform their own expensive fsync operations.

The function also includes a nudging mechanism: when the queue becomes more than half full, it signals the checkpointer via a latch to encourage it to process pending requests.

## Parameters / Member Variables
- ftag: Pointer to a FileTag structure identifying the specific file that needs to be synchronized
- type: SyncRequestType enumeration specifying the type of synchronization request

## Dependencies
- Functions called/Symbols referenced:
  - AmCheckpointerProcess
  - CompactCheckpointerRequestQueue  
  - [SetLatch](../S/SetLatch.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (CheckpointerCommLock)
- Types used:
  - [FileTag](FileTag.md)
  - SyncRequestType
  - [CheckpointerRequest](../C/CheckpointerRequest.md)
- Called from:
  - [RegisterSyncRequest](../R/RegisterSyncRequest.md)

## Notes and Other Information
- Returns false if the request cannot be queued (checkpointer not running, queue full and non-compactable), indicating the backend must perform its own fsync
- Returns true if the request was successfully queued
- Must not be called from within the checkpointer process itself (enforced by error check)
- Uses CheckpointerCommLock for thread-safe access to shared memory structures
- Includes statistical counting of direct backend writes for monitoring purposes
- The function will not execute under non-postmaster processes and returns false immediately in such cases

## Simplified Source

```c
bool ForwardSyncRequest(const FileTag *ftag, SyncRequestType type)
{
    CheckpointerRequest *request;
    bool too_full;

    if (!IsUnderPostmaster)
        return false;        // probably shouldn't even get here

    if (AmCheckpointerProcess())
        elog(ERROR, "ForwardSyncRequest must not be called in checkpointer");

    LWLockAcquire(CheckpointerCommLock, LW_EXCLUSIVE);

    // If the checkpointer isn't running or the request queue is full, the
    // backend will have to perform its own fsync request. But before forcing
    // that to happen, we can try to compact the request queue.
    if (CheckpointerShmem->checkpointer_pid == 0 ||
        (CheckpointerShmem->num_requests >= CheckpointerShmem->max_requests &&
         !CompactCheckpointerRequestQueue()))
    {
        LWLockRelease(CheckpointerCommLock);
        return false;
    }

    // OK, insert request
    request = &CheckpointerShmem->requests[CheckpointerShmem->num_requests++];
    request->ftag = *ftag;
    request->type = type;

    // If queue is more than half full, nudge the checkpointer to empty it
    too_full = (CheckpointerShmem->num_requests >=
                CheckpointerShmem->max_requests / 2);

    LWLockRelease(CheckpointerCommLock);

    // ... but not till after we release the lock
    if (too_full && ProcGlobal->checkpointerLatch)
        SetLatch(ProcGlobal->checkpointerLatch);

    return true;
}
```