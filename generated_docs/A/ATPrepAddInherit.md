# ATPrepAddInherit

## Location
[src/backend/commands/tablecmds.c:15639-15660](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L15639-L15660)

## Overview
Validates preconditions for ALTER TABLE INHERIT operations by checking that the child relation is eligible for inheritance modification.

## Definition

```c
static void
ATPrepAddInherit(Relation child_rel)
```
## Detailed Description
The  function performs preliminary validation checks for ALTER TABLE INHERIT operations. It ensures that the child relation is in a valid state to participate in table inheritance by verifying that it is not a typed table, partition, or partitioned table. This function is part of the preparation phase of ALTER TABLE command processing, which occurs before the actual inheritance relationship is established.

The function enforces PostgreSQL's inheritance rules by preventing inheritance modifications on relations that have special characteristics incompatible with traditional table inheritance.

## Parameters / Member Variables
- : The relation that is being prepared to inherit from another table

## Dependencies
- Functions called/Symbols referenced:
  - ereport (for error reporting)
  - [errcode](../e/errcode.md) (for error code specification)
  - [errmsg](../e/errmsg.md) (for error message formatting)
- Called from (representative examples):
  - [ATPrepCmd](ATPrepCmd.md) (during ALTER TABLE command preparation)

## Notes and Other Information
- Part of the ALTER TABLE command preparation infrastructure
- Enforces three key restrictions:
  - Typed tables (created with OF type_name) cannot participate in inheritance
  - Partitions cannot change their inheritance (they inherit from partition root)
  - Partitioned tables cannot participate in traditional inheritance
- This is a validation-only function that does not modify any state
- The actual inheritance establishment is handled by ATExecAddInherit
- These restrictions help maintain the integrity of PostgreSQL's type system and partitioning framework
- Failure in this function prevents the ALTER TABLE INHERIT command from proceeding

## Simplified Source

```c
static void
ATPrepAddInherit(Relation child_rel)
{
    // Check if table is a typed table (created with OF type_name)
    if (child_rel->rd_rel->reloftype)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                       errmsg("cannot change inheritance of typed table")));

    // Check if table is a partition
    if (child_rel->rd_rel->relispartition)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                       errmsg("cannot change inheritance of a partition")));

    // Check if table is a partitioned table
    if (child_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                       errmsg("cannot change inheritance of partitioned table")));
}
```