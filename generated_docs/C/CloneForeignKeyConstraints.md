# CloneForeignKeyConstraints

## Location
[src/backend/commands/tablecmds.c:10604-10632](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L10604-L10632)

## Overview
Clones foreign key constraints from a partitioned table to a newly acquired partition, handling both referencing-side and referenced-side constraints appropriately.

## Definition
```c
static void CloneForeignKeyConstraints(List **wqueue, Relation parentRel,
                                      Relation partitionRel)
```

## Detailed Description
This function is the main entry point for cloning foreign key constraints when a new partition is attached to a partitioned table. It serves as a coordinator that delegates the actual cloning work to specialized helper functions based on the constraint direction.

The function operates in two phases:
1. Clones constraints where the parent table is on the referencing side (foreign key side) via CloneFkReferencing
2. Clones constraints where the parent table is on the referenced side (primary key side) via CloneFkReferenced

This two-phase approach is necessary because foreign key constraints have bidirectional relationships - a table can be both the referencing side of some constraints and the referenced side of others. The function ensures that all foreign key relationships involving the parent table are properly replicated to the new partition.

The function assumes that the partition has the same column structure as the parent (same columns with same data types), though columns may be in different order. This assumption is valid because PostgreSQL's partitioning system enforces column compatibility between parent and partition relations.

## Parameters / Member Variables
- `wqueue`: ALTER TABLE work queue for scheduling Phase 3 constraint validation (can be NULL for cases like CREATE TABLE / PARTITION OF where the partition is known to be empty)
- `parentRel`: The partitioned parent table from which to clone constraints
- `partitionRel`: The newly acquired partition that should receive the cloned constraints

## Dependencies
- Functions called/Symbols referenced:
  - [CloneFkReferencing](CloneFkReferencing.md)
  - [CloneFkReferenced](CloneFkReferenced.md)
  - Assert (for validation)
- Called from (representative examples):
  - [DefineRelation](../D/DefineRelation.md)
  - [ATExecAttachPartition](../A/ATExecAttachPartition.md)

## Notes and Other Information
- This is a static function within tablecmds.c, part of the partitioning infrastructure
- The function only works with declarative partitioning (not legacy inheritance)
- The order of operations matters: referencing-side constraints are cloned first, then referenced-side constraints
- The work queue parameter is essential for proper constraint validation when the partition might contain existing data
- For newly created empty partitions (such as in CREATE TABLE / PARTITION OF), the work queue can be NULL since validation is not required
- This function is typically called during partition attachment operations to ensure constraint consistency across the partition hierarchy
- The function validates that the parent relation is indeed a partitioned table through an assertion
- Both helper functions handle the complexity of attribute mapping, trigger creation, and dependency management for their respective constraint sides

## Simplified Source

```c
static void CloneForeignKeyConstraints(List **wqueue, Relation parentRel,
                                      Relation partitionRel) {
    // Validate that parent is a partitioned table
    Assert(parentRel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE);

    // Clone constraints where parent is on the referencing side
    CloneFkReferencing(wqueue, parentRel, partitionRel);

    // Clone constraints where parent is on the referenced side
    CloneFkReferenced(parentRel, partitionRel);
}
```