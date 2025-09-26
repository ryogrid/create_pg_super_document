# ActiveSnapshotElt

## Location
[src/backend/utils/time/snapmgr.c:112-117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L112-L117)

## Overview
A data structure representing elements in PostgreSQL's active snapshot stack, managing the hierarchy of transaction snapshots and their nesting levels.

## Definition

```c
typedef struct ActiveSnapshotElt
{
	Snapshot	as_snap;
	int			as_level;
	struct ActiveSnapshotElt *as_next;
} ActiveSnapshotElt;
```
## Detailed Description
ActiveSnapshotElt is a fundamental component of PostgreSQL's snapshot management system, specifically designed to maintain a stack of active snapshots. Each element in this stack represents exactly one active_count reference on a SnapshotData structure. The stack is organized as a linked list where elements are maintained in non-increasing order of nesting level (as_level), ensuring proper hierarchical transaction management.

The structure supports PostgreSQL's multi-level transaction system by tracking which snapshot belongs to which transaction nesting level. When transactions are nested (such as with savepoints), this structure allows the system to maintain separate snapshots for different transaction levels while preserving the proper order for cleanup operations.

## Parameters / Member Variables
- `as_snap`: The actual Snapshot structure containing the snapshot data for this level
- `as_level`: The transaction nesting level that owns this snapshot (higher values indicate deeper nesting)
- `*as_next`: Pointer to the next element in the active snapshot stack (forms a linked list)
## Dependencies
- Functions called/Symbols referenced:
  - [Snapshot](../S/Snapshot.md) (data type)
  - struct ActiveSnapshotElt (self-reference for linked list)
  
- Called from (representative examples):
  - [PushActiveSnapshotWithLevel](../P/PushActiveSnapshotWithLevel.md) (creates new elements)
  - [PopActiveSnapshot](../P/PopActiveSnapshot.md) (removes elements)
  - [AtSubCommit_Snapshot](AtSubCommit_Snapshot.md) (transaction cleanup)
  - [AtSubAbort_Snapshot](AtSubAbort_Snapshot.md) (transaction rollback)
  - [AtEOXact_Snapshot](AtEOXact_Snapshot.md) (end-of-transaction cleanup)

## Notes and Other Information
- The active snapshot stack must always be NULL-terminated
- Elements are maintained in non-increasing order of as_level to support proper transaction nesting
- Each element accounts for exactly one active_count on the associated SnapshotData
- Memory allocation for elements occurs in TopTransactionContext to ensure proper lifetime management
- The structure is critical for supporting PostgreSQL's MVCC (Multi-Version Concurrency Control) system across nested transactions