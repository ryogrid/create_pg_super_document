# CheckRelationLockedByMe

## Location
[src/backend/storage/lmgr/lmgr.c:330-346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L330-L346)

## Overview
CheckRelationLockedByMe checks whether the current transaction holds a lock on the specified relation with the given lock mode or potentially stronger.

## Definition

```c
bool
CheckRelationLockedByMe(Relation relation, LOCKMODE lockmode, bool orstronger)
```
## Detailed Description
This function verifies if the current transaction has acquired a lock on the specified relation. It constructs a lock tag from the relation's database and relation identifiers, then delegates to LockHeldByMe to perform the actual lock check. The function can optionally check for stronger lock modes when the orstronger parameter is true, where "stronger" is defined numerically (higher LOCKMODE values).

## Parameters / Member Variables
- : The relation to check for lock ownership
- : The minimum lock mode to check for
- : If true, also accepts stronger (numerically higher) lock modes as satisfying the check

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_RELATION (macro to construct relation lock tag)
  - [LockHeldByMe](../L/LockHeldByMe.md) (performs the actual lock ownership check)
- Called from (representative examples):
  - [relation_open](../r/relation_open.md)
  - [try_relation_open](../t/try_relation_open.md)
  - [addFkRecurseReferenced](../a/addFkRecurseReferenced.md)
  - [addFkRecurseReferencing](../a/addFkRecurseReferencing.md)
  - [ExecGetRangeTableRelation](../E/ExecGetRangeTableRelation.md)

## Notes and Other Information
- Returns true if the current transaction holds the specified lock or stronger
- Uses the relation's lockRelId which contains both database ID and relation ID
- The "stronger" lock concept is semantically questionable but works for its intended purposes
- Located in src/backend/storage/lmgr/lmgr.c:330-346

## Simplified Source

```c
bool
CheckRelationLockedByMe(Relation relation, LOCKMODE lockmode, bool orstronger) {
    LOCKTAG tag;

    // Construct lock tag from relation's database and relation IDs
    SET_LOCKTAG_RELATION(tag,
                         relation->rd_lockInfo.lockRelId.dbId,
                         relation->rd_lockInfo.lockRelId.relId);

    // Check if current transaction holds the lock
    return LockHeldByMe(&tag, lockmode, orstronger);
}
```