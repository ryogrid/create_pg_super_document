# forcePartitionRootLoad

## Location
[src/bin/pg_dump/pg_dump.c:2628-2655](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L2628-L2655)

## Overview
Determines whether data loading must be forced through the partition root for a given partition table due to unsafe partitioning schemes in the hierarchy.

## Definition
```c
static bool forcePartitionRootLoad(const TableInfo *tbinfo)
```

## Detailed Description
This function traverses the partition hierarchy from a given partition up to its root to check if any ancestral partitioned table has an unsafe partitioning scheme. If any level of the hierarchy contains unsafe partitions, the function returns true, indicating that data must be loaded via the partition root rather than directly into individual partitions.

This is a safety mechanism in pg_dump to handle partitioned tables that may have been created with partitioning schemes that could cause data corruption or inconsistencies if data is loaded directly into partitions. By forcing load through the root, PostgreSQL can properly validate and route data to the correct partitions.

## Parameters / Member Variables
- `tbinfo`: A pointer to the TableInfo structure representing the partition table to check for unsafe partitioning ancestry

## Dependencies
- Functions called/Symbols referenced:
  - [TableInfo](../T/TableInfo.md) (structure type)
  - Assert (macro for debugging assertions)
- Called from (representative examples):
  - fmtQualifiedDumpable (src/bin/pg_dump/pg_dump.c:343)
  - [dumpTableData_insert](../d/dumpTableData_insert.md) (src/bin/pg_dump/pg_dump.c:2418)
  - [dumpTableData](../d/dumpTableData.md) (src/bin/pg_dump/pg_dump.c:2678)

## Notes and Other Information
- This is a static function, only accessible within pg_dump.c
- Returns boolean value: true if unsafe partitioning detected, false otherwise
- Essential for data integrity during pg_dump restore operations
- Complements getRootTableInfo by providing safety checks for partition hierarchies
- Uses the unsafe_partitions flag in TableInfo to make safety determinations
- Located at src/bin/pg_dump/pg_dump.c:2628-2655