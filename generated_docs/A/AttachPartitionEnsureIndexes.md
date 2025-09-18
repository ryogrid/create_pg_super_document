# AttachPartitionEnsureIndexes

## Location
[src/backend/commands/tablecmds.c:18803-18983](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L18803-L18983)

## Overview
AttachPartitionEnsureIndexes ensures that a partition being attached has all the required indexes that match the partitioned indexes on the parent table, either by finding existing compatible indexes or creating new ones.

## Definition
```c
static void AttachPartitionEnsureIndexes(List **wqueue, Relation rel, Relation attachrel)
```

## Detailed Description
This function enforces PostgreSQL's indexing rule for partitioned tables: every partition must have an index attached to each partitioned index on the parent table. The function operates in several phases:

**Index Discovery Phase:**
- Retrieves all indexes from both the parent partitioned table and the partition being attached
- Opens all existing indexes on the partition and builds IndexInfo structures for comparison
- Uses a temporary memory context for efficient memory management during the process

**Foreign Table Handling:**
- For foreign tables, validates that the parent has no unique or primary key indexes, as these cannot be enforced on foreign partitions
- Exits early for foreign tables after validation since no index creation is possible

**Index Matching and Creation Phase:**
For each partitioned index on the parent table:
- Searches for a compatible existing index on the partition using CompareIndexInfo
- Validates that both parent and child have matching constraint types if the index enforces a constraint
- If a compatible index is found, establishes the parent-child relationship via IndexSetParentIndex
- If no compatible index exists, creates a new index using generateClonedIndexStmt and DefineIndex

The function uses attribute mapping to handle differences in column ordering between parent and partition tables.

## Parameters / Member Variables
- `wqueue`: Work queue for ALTER TABLE operations (currently unused in this function)
- `rel`: The parent partitioned table relation
- `attachrel`: The partition relation being attached

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate, RelationGetIndexList, index_open, BuildIndexInfo
  - [build_attrmap_by_name](../b/build_attrmap_by_name.md), get_relation_idx_constraint_oid, CompareIndexInfo
  - [get_constraint_type](../g/get_constraint_type.md), IndexSetParentIndex, ConstraintSetParentConstraint
  - [generateClonedIndexStmt](../g/generateClonedIndexStmt.md), DefineIndex, CommandCounterIncrement
  - [index_close](../i/index_close.md), MemoryContextDelete
- Called from (representative examples):
  - [ATExecAttachPartition](ATExecAttachPartition.md)
  - child_dependency_type

## Notes and Other Information
- Static function used as a subroutine of ATExecAttachPartition
- Only processes partitioned indexes (RELKIND_PARTITIONED_INDEX), ignoring regular indexes on the parent
- Prevents attachment of foreign tables with unique indexes on the parent, as uniqueness cannot be enforced across foreign partitions  
- Uses AccessShareLock on indexes during the matching process to allow concurrent reads
- Creates a temporary memory context to manage memory efficiently during index processing
- Increments command counter after establishing parent-child relationships to ensure visibility
- Handles both constraint and non-constraint indexes appropriately
- Critical for maintaining index consistency across the partition hierarchy