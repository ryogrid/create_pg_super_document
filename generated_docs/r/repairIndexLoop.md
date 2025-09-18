# repairIndexLoop

## Location
[src/bin/pg_dump/pg_dump_sort.c:1135-1148](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump_sort.c#L1135-L1148)

## Overview
Repairs circular dependencies for index loops by removing the dependency from the partitioned index to its partition index.

## Definition
static void repairIndexLoop(DumpableObject *partedindex, DumpableObject *partindex)

## Detailed Description
This function is part of PostgreSQL's pg_dump dependency resolution system, specifically handling circular dependencies between partitioned indexes and their partition indexes. When a circular dependency is detected involving a partitioned index and one of its partition indexes, this function breaks the loop by removing the partitioned index's dependency on the partition index. This is necessary because partition indexes can create complex dependency relationships with their parent partitioned index that need to be resolved for proper dump ordering.

## Parameters / Member Variables
- `partedindex`: Pointer to the DumpableObject representing the partitioned index involved in the circular dependency
- `partindex`: Pointer to the DumpableObject representing the partition index that the partitioned index depends on

## Dependencies
- Functions called/Symbols referenced:
  - [removeObjectDependency](removeObjectDependency.md)
  - DumpableObject (struct type)
- Called from (representative examples):
  - [repairDependencyLoop](repairDependencyLoop.md) (at pg_dump_sort.c:1331)
  - [repairDependencyLoop](repairDependencyLoop.md) (at pg_dump_sort.c:1336)

## Notes and Other Information
- This is a static function within pg_dump_sort.c for internal dependency sorting use
- Specifically handles partitioned index dependencies, which can be complex due to the parent-child relationship
- Part of PostgreSQL's partitioning feature support in pg_dump
- The repair involves simply breaking the dependency from parent to child partition index
- Essential for proper dump ordering when dealing with partitioned tables and their associated indexes