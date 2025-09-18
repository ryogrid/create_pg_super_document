# ReindexPartitions

## Location
src/backend/commands/indexcmds.c: 3217 - 3310

## Overview
ReindexPartitions reindexes a set of partitions belonging to a partitioned table or partitioned index by processing each physical partition in separate transactions.

## Definition
```c
static void ReindexPartitions(const ReindexStmt *stmt, Oid relid, const ReindexParams *params, bool isTopLevel)
```

## Detailed Description
This function handles reindexing operations on partitioned relations by:

1. **Validation**: Verifies the target relation is partitioned using RELKIND_HAS_PARTITIONS macro
2. **Error Context Setup**: Establishes error callback context for enhanced error reporting during partition processing
3. **Transaction Block Prevention**: Ensures the operation cannot run within a user transaction block since it commits internally
4. **Partition Discovery**: Uses find_all_inheritors() to locate all partitions in the inheritance hierarchy
5. **Physical Partition Filtering**: Filters out partitioned tables/indexes and foreign tables, keeping only relations with physical storage (RELKIND_INDEX and RELKIND_RELATION)
6. **Memory Management**: Creates a separate memory context for cross-transaction storage of partition OIDs
7. **Delegated Processing**: Passes the filtered list to ReindexMultipleInternal() for actual reindexing

## Parameters / Member Variables
- `stmt`: ReindexStmt containing the reindex statement details
- `relid`: OID of the partitioned table or partitioned index to reindex
- `params`: ReindexParams specifying reindex options and parameters
- `isTopLevel`: Boolean indicating if this is a top-level operation (affects transaction block prevention)

## Dependencies
- Functions called/Symbols referenced:
  - get_rel_relkind
  - get_rel_name  
  - get_rel_namespace
  - get_namespace_name
  - reindex_error_callback
  - PreventInTransactionBlock
  - AllocSetContextCreate
  - find_all_inheritors
  - ReindexMultipleInternal
  - MemoryContextDelete
- Called from:
  - ReindexIndex
  - ReindexTable

## Notes and Other Information
- The function uses ShareLock to prevent schema modifications during partition discovery
- Only processes partitions with physical storage (excludes partitioned tables/indexes and foreign tables)
- Each partition is processed in a separate transaction to reduce deadlock risk and enable immediate lock release
- Error context includes qualified relation name for precise error identification
- Memory context management ensures cleanup even in error scenarios since it"s a child of PortalContext
- The function specifically handles both partitioned tables (REINDEX TABLE) and partitioned indexes (REINDEX INDEX) scenarios in transaction block prevention