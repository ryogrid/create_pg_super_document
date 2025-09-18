# ReindexTable

## Location
[src/backend/commands/indexcmds.c:2918-2976](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/indexcmds.c#L2918-L2976)

## Overview
ReindexTable recreates all indexes of a table (including its toast table indexes if any) with appropriate handling for partitioned tables, concurrent operations, and temporary tables.

## Definition
```c
static Oid ReindexTable(const ReindexStmt *stmt, const ReindexParams *params, bool isTopLevel)
```

## Detailed Description
This function orchestrates the reindexing of an entire table by:

1. **Lock Acquisition**: Acquires appropriate locks on the target table based on whether concurrent reindexing is requested (ShareUpdateExclusiveLock for concurrent, ShareLock for non-concurrent)
2. **Relation Type Handling**: Dispatches to specialized handlers based on table type:
   - Partitioned tables: Delegates to ReindexPartitions()
   - Regular tables with concurrent option: Uses ReindexRelationConcurrently()
   - Regular tables without concurrent option: Uses reindex_relation()
3. **Temporary Table Handling**: Forces non-concurrent reindexing for temporary tables even if CONCURRENTLY was requested
4. **Progress Reporting**: Enables progress reporting for non-concurrent operations
5. **Toast Table Processing**: Includes toast table indexes in the reindexing process for non-concurrent operations

## Parameters / Member Variables
- `stmt`: ReindexStmt containing the reindex statement details including target relation
- `params`: ReindexParams specifying reindex options such as concurrency settings
- `isTopLevel`: Boolean indicating if this is a top-level operation (affects behavior in recursive contexts)

## Dependencies
- Functions called/Symbols referenced:
  - [RangeVarGetRelidExtended](RangeVarGetRelidExtended.md)
  - [RangeVarCallbackMaintainsTable](RangeVarCallbackMaintainsTable.md)
  - [get_rel_relkind](../g/get_rel_relkind.md)
  - [get_rel_persistence](../g/get_rel_persistence.md)
  - [ReindexPartitions](ReindexPartitions.md)
  - ReindexRelationConcurrently
  - [reindex_relation](../r/reindex_relation.md)
- Called from:
  - [ExecReindex](../E/ExecReindex.md)

## Notes and Other Information
- The function automatically upgrades locks for temporary tables when concurrent reindexing is requested
- For partitioned tables, it delegates the entire operation to ReindexPartitions rather than handling partitions individually
- Progress reporting is automatically enabled for non-concurrent operations to provide user feedback
- The function includes toast table processing (REINDEX_REL_PROCESS_TOAST) and constraint checking (REINDEX_REL_CHECK_CONSTRAINTS) for comprehensive reindexing
- Returns the OID of the reindexed table for use by calling functions