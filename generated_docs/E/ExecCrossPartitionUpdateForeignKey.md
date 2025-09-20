# ExecCrossPartitionUpdateForeignKey

## Location
[src/backend/executor/nodeModifyTable.c:2200-2291](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L2200-L2291)

## Overview
ExecCrossPartitionUpdateForeignKey ensures foreign key constraint integrity during cross-partition updates by firing appropriate triggers and validating foreign key relationships.

## Definition

```c
static void
ExecCrossPartitionUpdateForeignKey(ModifyTableContext *context,
								   ResultRelInfo *sourcePartInfo,
								   ResultRelInfo *destPartInfo,
								   ItemPointer tupleid,
								   TupleTableSlot *oldslot,
								   TupleTableSlot *newslot)
```
## Detailed Description
ExecCrossPartitionUpdateForeignKey handles the complex scenario of maintaining foreign key constraints when a tuple is moved from one partition to another during an update operation. The function performs several critical validations:

1. **Ancestor Relationship Analysis**: Identifies all ancestor relations of the source partition using ExecGetAncestorResultRels
2. **Foreign Key Constraint Validation**: Checks for foreign keys pointing to non-root ancestors of the source partition, which cannot be properly enforced during cross-partition moves
3. **Error Reporting**: Reports detailed errors when unsupported foreign key configurations are detected, providing specific guidance on how to resolve the issue
4. **Root Trigger Execution**: Fires the root table's AFTER ROW UPDATE triggers to ensure foreign key constraints are properly validated for the cross-partition update

The function is essential for maintaining referential integrity when updates cause tuples to move between partitions, as the standard UPDATE trigger mechanisms may not be sufficient to handle the delete-from-source-partition + insert-to-destination-partition semantics.

## Parameters / Member Variables
- : ModifyTableContext containing execution state and metadata
- : ResultRelInfo for the partition containing the original tuple
- : ResultRelInfo for the partition that will contain the updated tuple
- : ItemPointer identifying the original tuple location
- : TupleTableSlot containing the original tuple data
- : TupleTableSlot containing the updated tuple data

## Dependencies
- Functions called/Symbols referenced:
  - [ExecGetAncestorResultRels](ExecGetAncestorResultRels.md)
  - [RI_FKey_trigger_type](../R/RI_FKey_trigger_type.md)
  - RI_TRIGGER_PK
  - [ExecARUpdateTriggers](ExecARUpdateTriggers.md)
  - RelationGetRelationName
  - ereport
- Called from (representative examples):
  - [ExecUpdateAct](ExecUpdateAct.md) (src/backend/executor/nodeModifyTable.c:2093)

## Notes and Other Information
- The function is static and only used within nodeModifyTable.c
- Cross-partition updates are implemented as a DELETE from the source partition followed by an INSERT into the destination partition
- Foreign keys pointing to non-root ancestors cannot be properly enforced during cross-partition moves, resulting in a FEATURE_NOT_SUPPORTED error
- The error provides helpful guidance suggesting users define foreign keys on the root partitioned table instead
- Only non-cloned foreign key triggers are considered problematic, as cloned triggers are inherited from parent tables
- The root table's AFTER ROW UPDATE triggers are fired with special parameters indicating this is a cross-partition update scenario
- This function is only called when both the source partition has UPDATE triggers and a successful cross-partition move has occurred