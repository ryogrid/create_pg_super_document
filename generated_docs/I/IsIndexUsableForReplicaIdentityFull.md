# IsIndexUsableForReplicaIdentityFull

## Location
src/backend/replication/logical/relation.c: 804 - 850

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
  - IndexInfo
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