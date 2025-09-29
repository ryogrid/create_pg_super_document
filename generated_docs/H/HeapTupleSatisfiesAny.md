# HeapTupleSatisfiesAny

## Location
[src/backend/access/heap/heapam_visibility.c:340-361](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_visibility.c#L340-L361)

## Overview
A trivial visibility function that considers all tuples visible, used with the SnapshotAny special snapshot type for operations that need to see all tuples regardless of their transaction status.

## Definition

```c
static bool
HeapTupleSatisfiesAny(HeapTuple htup, Snapshot snapshot, Buffer buffer)
```
## Detailed Description
HeapTupleSatisfiesAny is the simplest visibility function in PostgreSQL's MVCC system. It unconditionally returns true, meaning every tuple is considered visible regardless of its transaction state, commit status, or any other factors.

This function is used with the SnapshotAny snapshot type, which is employed in special administrative or maintenance operations where the system needs to access all tuples in a table regardless of their visibility under normal MVCC rules. Examples include certain VACUUM operations, system catalog maintenance, and recovery processes.

The function serves as a "bypass" for the normal MVCC visibility checks, allowing callers to iterate over all physical tuples in a table without filtering based on transaction visibility.

## Parameters / Member Variables
- `htup`: The heap tuple to check (ignored, as all tuples are considered visible)
- `snapshot`: The snapshot context (ignored, but should be SnapshotAny)
- `buffer`: Buffer containing the tuple (ignored)

## Dependencies
- Functions called/Symbols referenced: None
- Called from (representative examples):
  - [HeapTupleSatisfiesVisibility](HeapTupleSatisfiesVisibility.md)

## Notes and Other Information
- This is a dummy function that always returns true
- Used exclusively with SnapshotAny for administrative operations
- Provides a consistent interface with other visibility functions while bypassing all MVCC checks
- Essential for operations that need to see deleted, uncommitted, or otherwise invisible tuples
- Part of PostgreSQL's pluggable tuple visibility system architecture

## Simplified Source

```c
static bool HeapTupleSatisfiesAny(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
    // Always return true - every tuple is visible with SnapshotAny
    return true;
}
```