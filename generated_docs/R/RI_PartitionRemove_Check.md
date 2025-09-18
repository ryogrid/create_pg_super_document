# RI_PartitionRemove_Check

## Location
src/backend/utils/adt/ri_triggers.c: 1654 - 1872

## Overview
Verifies that no foreign key references exist when a partition is detached from the referenced side of a foreign key constraint.

## Definition
```c
void RI_PartitionRemove_Check(Trigger *trigger, Relation fk_rel, Relation pk_rel)
```

## Detailed Description
This function performs referential integrity validation specifically for partition detachment operations on the referenced (primary key) side of a foreign key constraint. When a partition containing primary key values is about to be detached, this function ensures no foreign key rows in other tables would be left referencing non-existent primary key values.

The function constructs and executes a specialized INNER JOIN query that:
1. **Constraint Discovery**: Uses the partition constraint to identify rows that would be removed
2. **Reference Detection**: Finds foreign key rows that reference values in the partition being detached
3. **Query Construction**: Builds a query with partition constraint filtering
4. **Match Type Logic**: Handles different NULL behaviors (MATCH SIMPLE vs MATCH FULL)
5. **Performance Optimization**: Temporarily increases work_mem for efficient execution
6. **Violation Reporting**: Reports detailed constraint violation if any references exist

The generated query structure is:
```sql
SELECT fk.keycols FROM [ONLY] fk_table fk
JOIN pk_partition pk ON (pk.key = fk.key)
WHERE (<partition constraint>) AND (fk.key IS NOT NULL [AND/OR ...])
```

## Parameters / Member Variables
- `trigger`: The foreign key trigger containing constraint information
- `fk_rel`: The foreign key table relation that references the partition
- `pk_rel`: The partition being detached from the primary key table

## Dependencies
- Functions called/Symbols referenced:
  - [ri_FetchConstraintInfo](../r/ri_FetchConstraintInfo.md)
  - [quoteOneName](../q/quoteOneName.md), quoteRelationName
  - RIAttName, RIAttType, RIAttCollation
  - [ri_GenerateQual](../r/ri_GenerateQual.md), ri_GenerateQualCollation
  - [pg_get_partconstrdef_string](../p/pg_get_partconstrdef_string.md)
  - SPI_connect, SPI_prepare, SPI_execute_snapshot, SPI_finish
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)
  - [heap_deform_tuple](../h/heap_deform_tuple.md), ExecStoreVirtualTuple
  - [ri_ReportViolation](../r/ri_ReportViolation.md)
  - [NewGUCNestLevel](../N/NewGUCNestLevel.md), set_config_option, AtEOXact_GUC
- Called from (representative examples):
  - [ATDetachCheckNoForeignKeyRefs](../A/ATDetachCheckNoForeignKeyRefs.md)

## Notes and Other Information
- This is a specialized function for partition management operations, not general constraint checking
- Does not perform permission checks, assuming the user detaching has sufficient privileges
- Uses INNER JOIN (not LEFT OUTER JOIN) to find existing references rather than missing ones
- Incorporates partition constraint logic to identify which rows would be affected by detachment
- Temporarily adjusts work_mem and hash_mem_multiplier for performance optimization
- Located in src/backend/utils/adt/ri_triggers.c:1654-1872
- Returns void but throws an error if any referencing rows are found
- Part of PostgreSQL's partition management and referential integrity system
- Handles both partitioned and regular foreign key tables appropriately