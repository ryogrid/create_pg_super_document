# getRootTableInfo

## Location
[src/bin/pg_dump/pg_dump.c:2603-2627](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L2603-L2627)

## Overview
Retrieves the root (top-level) TableInfo for a given partition table by traversing the partition hierarchy upwards.

## Definition

```c
static TableInfo *
getRootTableInfo(const TableInfo *tbinfo)
```
## Detailed Description
This function traverses the partition hierarchy from a given partition table to find its root table. It follows the parent-child relationships defined in the partition tree, moving upward from child partitions through intermediate partitioned tables until it reaches the root table (which is not a partition itself). This is essential for pg_dump operations where understanding the complete partition hierarchy is necessary for proper data dumping and schema recreation.

The function uses assertions to ensure the input table is indeed a partition with exactly one parent, maintaining the integrity of PostgreSQL's partition hierarchy constraints.

## Parameters / Member Variables
- `*tbinfo`: A pointer to the TableInfo structure representing the partition table whose root needs to be found
## Dependencies
- Functions called/Symbols referenced:
  - [TableInfo](../T/TableInfo.md) (structure type)
  - Assert (macro for debugging assertions)
- Called from (representative examples):
  - fmtQualifiedDumpable (src/bin/pg_dump/pg_dump.c:342)
  - [dumpTableData_insert](../d/dumpTableData_insert.md) (src/bin/pg_dump/pg_dump.c:2419)  
  - [dumpTableData](../d/dumpTableData.md) (src/bin/pg_dump/pg_dump.c:2683)

## Notes and Other Information
- This is a static function, meaning it's only accessible within pg_dump.c
- The function assumes PostgreSQL's partition hierarchy constraints (each partition has exactly one parent)
- Uses assertions that will only trigger in debug builds to validate input assumptions
- Essential for proper handling of partitioned tables in pg_dump operations
- Located at src/bin/pg_dump/pg_dump.c:2603-2627