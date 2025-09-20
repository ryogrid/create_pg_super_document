# PredicateLockingNeededForRelation

## Location
[src/backend/storage/lmgr/predicate.c:498-515](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L498-L515)

## Overview
Determines whether a given relation should participate in predicate locking as part of PostgreSQL's serializable snapshot isolation implementation.

## Definition

```c
static inline bool
PredicateLockingNeededForRelation(Relation relation)
```
## Detailed Description
This function serves as a gatekeeper for predicate locking by checking whether a relation requires predicate locks for serializable transaction isolation. It implements an optimization by excluding temporary relations and system catalogs from predicate locking, as these typically don't require the overhead of serialization conflict detection.

The function performs two key checks:
1. **System relation check**: Relations with OIDs below FirstUnpinnedObjectId are system catalogs
2. **Temporary relation check**: Relations that use local buffers are temporary tables

Both types of relations are exempt from predicate locking because:
- System catalogs are rarely modified in ways that would cause serialization conflicts
- Temporary relations are session-local and cannot be accessed by other transactions

## Parameters / Member Variables
- : The Relation structure representing the table/index to check for predicate locking eligibility

## Dependencies
- Functions called/Symbols referenced:
  - FirstUnpinnedObjectId (constant defining the boundary between system and user objects)
  - RelationUsesLocalBuffers (function to check if relation uses local/temporary buffers)
- Called from (representative examples):
  - [SerializationNeededForRead](../S/SerializationNeededForRead.md)
  - [SerializationNeededForWrite](../S/SerializationNeededForWrite.md)
  - [DropAllPredicateLocksFromTable](../D/DropAllPredicateLocksFromTable.md)
  - [PredicateLockPageSplit](PredicateLockPageSplit.md)

## Notes and Other Information
- This is a static inline function for performance, as it's called frequently during predicate lock operations
- The exclusion of system relations and temporary tables is a significant optimization that reduces predicate locking overhead
- Part of PostgreSQL's Serializable Snapshot Isolation (SSI) implementation for preventing serialization anomalies