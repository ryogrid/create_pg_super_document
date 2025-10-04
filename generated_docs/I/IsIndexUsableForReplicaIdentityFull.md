# IsIndexUsableForReplicaIdentityFull

## Location
[src/backend/replication/logical/relation.c:804-850](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/relation.c#L804-L850)

## Overview
Determines whether a specific index can be used for replica identity full operations in logical replication by evaluating access method, structure, and attribute mapping requirements.

## Definition
```c
bool IsIndexUsableForReplicaIdentityFull(IndexInfo *indexInfo, AttrMap *attrmap)
```

## Detailed Description
This function evaluates whether an index meets the strict criteria required for use in replica identity full scenarios. It performs several validation checks to ensure the index can support efficient row identification and comparison operations needed in logical replication.

The function enforces several key requirements: the index must use a btree or hash access method with a valid equality strategy, it cannot be a partial index, the leftmost index column must be a simple column reference (not an expression), and that column must map to a corresponding remote relation attribute. These restrictions ensure the index scan behavior is similar to primary key or replica identity index scans while maintaining compatibility with tuple comparison operations.

The design accommodates the specific needs of logical replication where full row images are used for tuple identification, allowing for more flexibility than primary key constraints while still ensuring reliable and efficient index-based lookups.

## Parameters / Member Variables
- `indexInfo`: IndexInfo structure containing metadata about the index being evaluated
- `attrmap`: AttrMap structure mapping local relation attributes to remote relation attributes

## Dependencies
- Functions called/Symbols referenced:
  - [get_equal_strategy_number_for_am](../g/get_equal_strategy_number_for_am.md)
  - AttributeNumberIsValid
  - AttrNumberGetAttrOffset
  - [GetIndexAmRoutineByAmId](../G/GetIndexAmRoutineByAmId.md) (in assertion code)
- Types referenced:
  - [IndexInfo](IndexInfo.md)
  - [AttrMap](../A/AttrMap.md)
  - AttrNumber
  - [IndexAmRoutine](IndexAmRoutine.md) (in assertion code)
- Constants referenced:
  - InvalidStrategy
  - NIL
- Called from (representative examples):
  - [FindUsableIndexForReplicaIdentityFull](../F/FindUsableIndexForReplicaIdentityFull.md)
  - [FindReplTupleInLocalRel](../F/FindReplTupleInLocalRel.md)

## Notes and Other Information
- Only supports btree and hash indexes due to fixed equality strategy requirements and default operator class availability
- Excludes partial indexes to avoid complex fallback scenarios when index conditions are not satisfied
- Requires leftmost index column to be a simple column reference, not an expression, for predictable scan behavior
- The leftmost column must have a corresponding remote attribute to ensure meaningful cross-relation comparisons
- More permissive than primary key constraints (allows NULLs, doesn't require NOT DEFERRABLE) since tuple comparison handles edge cases
- BRIN and GIN indexes are excluded because they don't implement the required amgettuple interface
- Contains assertion code to verify the access method implements amgettuple when USE_ASSERT_CHECKING is enabled
- Designed to keep index scans similar to primary key/replica identity scans while supporting the broader requirements of REPLICA IDENTITY FULL

## Simplified Source

```c
bool
IsIndexUsableForReplicaIdentityFull(IndexInfo *indexInfo, AttrMap *attrmap)
{
    AttrNumber keycol;

    // Index must have valid equality strategy (btree or hash)
    if (get_equal_strategy_number_for_am(indexInfo->ii_Am) == InvalidStrategy)
        return false;

    // Index must not be partial
    if (indexInfo->ii_Predicate != NIL)
        return false;

    Assert(indexInfo->ii_NumIndexAttrs >= 1);

    // Leftmost column must be a simple column reference (not expression)
    keycol = indexInfo->ii_IndexAttrNumbers[0];
    if (!AttributeNumberIsValid(keycol))
        return false;

    // Leftmost column must map to a remote relation attribute
    if (attrmap->maplen <= AttrNumberGetAttrOffset(keycol) ||
        attrmap->attnums[AttrNumberGetAttrOffset(keycol)] < 0)
        return false;

#ifdef USE_ASSERT_CHECKING
    {
        IndexAmRoutine *amroutine;
        // Verify access method implements amgettuple
        amroutine = GetIndexAmRoutineByAmId(indexInfo->ii_Am, false);
        Assert(amroutine->amgettuple != NULL);
    }
#endif

    return true;
}
```