# RWConflictExists

## Location
[src/backend/storage/lmgr/predicate.c:610-642](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L610-L642)

## Overview
Checks whether a read-write conflict exists between two serializable transactions by examining their conflict lists.

## Definition

```c
static bool
RWConflictExists(const SERIALIZABLEXACT *reader, const SERIALIZABLEXACT *writer)
```
## Detailed Description
This function determines if there is already an existing read-write conflict between a reader transaction and a writer transaction. It performs this check by iterating through the reader's outgoing conflicts list to see if any conflict points to the specified writer transaction. The function includes optimizations to quickly return false in cases where conflicts are impossible (e.g., when either transaction is doomed or when the relevant conflict lists are empty).

## Parameters / Member Variables
- : Pointer to the serializable transaction that is reading data
- : Pointer to the serializable transaction that is writing data

## Dependencies
- Functions called/Symbols referenced:
  - SxactIsDoomed
  - [dlist_is_empty](../d/dlist_is_empty.md)
  - dlist_foreach
  - dlist_container
  - unconstify
- Types referenced:
  - [SERIALIZABLEXACT](../S/SERIALIZABLEXACT.md)
  - [dlist_iter](../d/dlist_iter.md)
  - [RWConflict](RWConflict.md)
  - [RWConflictData](RWConflictData.md)
- Called from (representative examples):
  - [SetRWConflict](../S/SetRWConflict.md)
  - [CheckForSerializableConflictOut](../C/CheckForSerializableConflictOut.md)
  - [CheckTargetForConflictsIn](../C/CheckTargetForConflictsIn.md)
  - [CheckTableForSerializableConflictIn](../C/CheckTableForSerializableConflictIn.md)

## Notes and Other Information
- Returns true if a conflict exists, false otherwise
- Includes early exit optimizations for performance
- Uses unconstify to work around const restrictions in dlist_foreach
- Part of PostgreSQL's serializable snapshot isolation implementation
- Located in src/backend/storage/lmgr/predicate.c:610-642

## Simplified Source

```c
static bool
RWConflictExists(const SERIALIZABLEXACT *reader, const SERIALIZABLEXACT *writer)
{
    dlist_iter iter;

    Assert(reader != writer);

    // Quick checks for impossible conflicts
    if (SxactIsDoomed(reader) ||
        SxactIsDoomed(writer) ||
        dlist_is_empty(&reader->outConflicts) ||
        dlist_is_empty(&writer->inConflicts)) {
        return false;
    }

    // Search through reader's outgoing conflicts
    dlist_foreach(iter, &unconstify(SERIALIZABLEXACT *, reader)->outConflicts) {
        RWConflict conflict = dlist_container(RWConflictData, outLink, iter.cur);

        if (conflict->sxactIn == writer)
            return true;
    }

    return false;
}
```