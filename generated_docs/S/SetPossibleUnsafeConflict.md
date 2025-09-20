# SetPossibleUnsafeConflict

## Location
[src/backend/storage/lmgr/predicate.c:666-690](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L666-L690)

## Overview
Records a potential unsafe conflict between a read-only transaction and an active read-write transaction for later evaluation during serialization conflict detection.

## Definition

```c
static void
SetPossibleUnsafeConflict(SERIALIZABLEXACT *roXact,
						  SERIALIZABLEXACT *activeXact)
```
## Detailed Description
This function creates a record of a possible unsafe conflict between a read-only transaction and an active (read-write) transaction. Unlike regular read-write conflicts, these are potential conflicts that may become actual serialization violations depending on future transaction behavior. The conflict record is allocated from the RWConflictPool and added to the possibleUnsafeConflicts lists of both transactions for later analysis.

## Parameters / Member Variables
- : Pointer to the read-only serializable transaction
- : Pointer to the active (read-write) serializable transaction

## Dependencies
- Functions called/Symbols referenced:
  - SxactIsReadOnly
  - [dlist_is_empty](../d/dlist_is_empty.md)
  - dlist_head_element
  - [dlist_delete](../d/dlist_delete.md)
  - [dlist_push_tail](../d/dlist_push_tail.md)
  - ereport
- Types referenced:
  - [SERIALIZABLEXACT](SERIALIZABLEXACT.md)
  - [RWConflict](../R/RWConflict.md)
  - [RWConflictData](../R/RWConflictData.md)
- Global variables accessed:
  - RWConflictPool
- Called from (representative examples):
  - [GetSerializableTransactionSnapshotInt](../G/GetSerializableTransactionSnapshotInt.md)

## Notes and Other Information
- Asserts that the transactions are different and have the expected read-only/read-write characteristics
- Uses SxactIsReadOnly to verify transaction types
- Raises an ERROR if the RWConflictPool is exhausted
- Adds the conflict to possibleUnsafeConflicts lists rather than regular conflict lists
- Part of PostgreSQL's serializable snapshot isolation safe snapshot optimization
- Located in src/backend/storage/lmgr/predicate.c:666-690