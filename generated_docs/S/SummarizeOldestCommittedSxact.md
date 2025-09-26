# SummarizeOldestCommittedSxact

## Location
[src/backend/storage/lmgr/predicate.c:1493-1547](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L1493-L1547)

## Overview
Frees shared memory by converting the oldest committed serializable transaction into summary form and releasing associated data structures.

## Definition
static void SummarizeOldestCommittedSxact(void)

## Detailed Description
This function is a critical memory management component of PostgreSQL's Serializable Snapshot Isolation (SSI) implementation. When the system runs out of available SERIALIZABLEXACT slots, this function reclaims memory by processing the oldest completed transaction from the FinishedSerializableTransactions queue.

The function performs the following operations:
1. Acquires the SerializableFinishedListLock to ensure exclusive access
2. Checks if there are any finished transactions to process (handles race conditions)
3. Removes the oldest transaction from the finished transactions list
4. Adds relevant conflict information to the SLRU (Simple LRU) summary
5. Releases all detailed structures associated with the transaction
6. Releases the lock

The function handles race conditions where another backend might have already cleaned up finished transactions while this function was waiting for locks.

## Parameters / Member Variables
- No parameters (void function)
- Works on global shared memory structures:
  - FinishedSerializableTransactions list
  - SERIALIZABLEXACT structures
  - Associated SLRU summary data

## Dependencies
- Functions called/Symbols referenced:
  - SERIALIZABLEXACT
  - dlist_is_empty
  - dlist_head_element
  - dlist_delete_thoroughly
  - SxactIsReadOnly
  - SxactHasConflictOut
  - SerialAdd
  - InvalidSerCommitSeqNo
  - ReleaseOneSerializableXact
- Called from (representative examples):
  - SerialControl (during initialization)
  - GetSerializableTransactionSnapshotInt (when slots are exhausted)

## Notes and Other Information
- This is a static function local to predicate.c
- Called only when SERIALIZABLEXACT slots are exhausted
- Handles race conditions gracefully by checking for empty finished list
- May free multiple associated structures beyond just SERIALIZABLEXACT
- Part of the SSI memory management and cleanup subsystem
- Uses SLRU for long-term storage of summarized conflict information
- Located in src/backend/storage/lmgr/predicate.c:1493-1547