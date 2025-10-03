# SetRWConflict

## Location
[src/backend/storage/lmgr/predicate.c:643-665](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L643-L665)

## Overview
Creates and records a read-write conflict between two serializable transactions by allocating a conflict record from the pool and linking it to both transactions.

## Definition

```c
static void
SetRWConflict(SERIALIZABLEXACT *reader, SERIALIZABLEXACT *writer)
```
## Detailed Description
This function establishes a new read-write conflict relationship between a reader transaction and a writer transaction. It allocates a conflict record from the RWConflictPool, initializes it with pointers to both transactions, and adds the conflict to the appropriate conflict lists of both transactions. The function includes safety checks to ensure the conflict doesn't already exist and that pool resources are available.

## Parameters / Member Variables
- `*reader`: Pointer to the serializable transaction that is reading data
- `*writer`: Pointer to the serializable transaction that is writing data
## Dependencies
- Functions called/Symbols referenced:
  - [RWConflictExists](../R/RWConflictExists.md)
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
  - [FlagRWConflict](../F/FlagRWConflict.md)

## Notes and Other Information
- Asserts that reader and writer are different transactions
- Asserts that the conflict doesn't already exist using RWConflictExists
- Raises an ERROR if the RWConflictPool is exhausted
- Adds the conflict to both the reader's outConflicts list and writer's inConflicts list
- Part of PostgreSQL's serializable snapshot isolation conflict detection mechanism
- Located in src/backend/storage/lmgr/predicate.c:643-665

## Simplified Source

```c
static void
SetRWConflict(SERIALIZABLEXACT *reader, SERIALIZABLEXACT *writer)
{
    RWConflict conflict;

    // Validate input parameters
    Assert(reader != writer);
    Assert(!RWConflictExists(reader, writer));

    // Check if conflict pool has available entries
    if (dlist_is_empty(&RWConflictPool->availableList))
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("not enough elements in RWConflictPool"),
                 errhint("Consider reducing concurrent transactions or increasing max_connections")));

    // Allocate conflict record from pool
    conflict = dlist_head_element(RWConflictData, outLink, &RWConflictPool->availableList);
    dlist_delete(&conflict->outLink);

    // Initialize conflict with transaction pointers
    conflict->sxactOut = reader;
    conflict->sxactIn = writer;

    // Link conflict to both transactions
    dlist_push_tail(&reader->outConflicts, &conflict->outLink);
    dlist_push_tail(&writer->inConflicts, &conflict->inLink);
}
```