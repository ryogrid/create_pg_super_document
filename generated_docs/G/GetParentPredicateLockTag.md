# GetParentPredicateLockTag

## Location
[src/backend/storage/lmgr/predicate.c:2062-2100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L2062-L2100)

## Overview
Returns the parent lock tag in PostgreSQL's predicate lock hierarchy, providing the next coarser granularity lock that covers the specified lock target.

## Definition

```c
static bool
GetParentPredicateLockTag(const PREDICATELOCKTARGETTAG *tag,
						  PREDICATELOCKTARGETTAG *parent)
```
## Detailed Description
This function implements the hierarchical relationship in PostgreSQL's predicate locking system. It determines the parent lock for a given lock target based on the lock granularity hierarchy: tuple locks have page parents, page locks have relation parents, and relation locks have no parent. The function extracts the lock type from the input tag and constructs the appropriate parent tag by preserving database and relation identifiers while adjusting the granularity level.

The lock hierarchy follows this pattern:
- TUPLE locks → PAGE locks (parent)
- PAGE locks → RELATION locks (parent)  
- RELATION locks → no parent

## Parameters / Member Variables
- `*tag`: A pointer to the predicate lock target tag for which to find the parent
- `*parent`: A pointer to the structure that will be populated with the parent tag information
## Dependencies
- Functions called/Symbols referenced:
  - GET_PREDICATELOCKTARGETTAG_TYPE
  - SET_PREDICATELOCKTARGETTAG_RELATION
  - SET_PREDICATELOCKTARGETTAG_PAGE
  - GET_PREDICATELOCKTARGETTAG_DB
  - GET_PREDICATELOCKTARGETTAG_RELATION
  - GET_PREDICATELOCKTARGETTAG_PAGE
- Called from (representative examples):
  - [SerialControl](../S/SerialControl.md)
  - [CoarserLockCovers](../C/CoarserLockCovers.md)
  - [CheckAndPromotePredicateLockRequest](../C/CheckAndPromotePredicateLockRequest.md)
  - [DecrementParentLocks](../D/DecrementParentLocks.md)
  - [PredicateLockPageSplit](../P/PredicateLockPageSplit.md)

## Notes and Other Information
- Static function, only accessible within predicate.c
- Returns true if a parent exists and sets the parent parameter, false if no parent exists
- [Relation](../R/Relation.md)-level locks are the coarsest granularity and have no parent
- Essential for lock escalation and promotion in the predicate locking system
- Uses assertion to ensure all valid lock types are handled
- Part of PostgreSQL's serializable snapshot isolation implementation

## Simplified Source

```c
static bool GetParentPredicateLockTag(const PREDICATELOCKTARGETTAG *tag,
                                      PREDICATELOCKTARGETTAG *parent) {
    switch (GET_PREDICATELOCKTARGETTAG_TYPE(*tag)) {
        case PREDLOCKTAG_RELATION:
            // Relation locks are top-level, no parent
            return false;

        case PREDLOCKTAG_PAGE:
            // Parent of page lock is relation lock
            SET_PREDICATELOCKTARGETTAG_RELATION(*parent,
                                                GET_PREDICATELOCKTARGETTAG_DB(*tag),
                                                GET_PREDICATELOCKTARGETTAG_RELATION(*tag));
            return true;

        case PREDLOCKTAG_TUPLE:
            // Parent of tuple lock is page lock
            SET_PREDICATELOCKTARGETTAG_PAGE(*parent,
                                            GET_PREDICATELOCKTARGETTAG_DB(*tag),
                                            GET_PREDICATELOCKTARGETTAG_RELATION(*tag),
                                            GET_PREDICATELOCKTARGETTAG_PAGE(*tag));
            return true;
    }

    // Should never reach here
    Assert(false);
    return false;
}
```