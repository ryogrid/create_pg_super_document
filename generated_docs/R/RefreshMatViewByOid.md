# RefreshMatViewByOid

## Location
src/backend/commands/matview.c: 162 - 388

## Overview
RefreshMatViewByOid performs the core materialized view refresh operation by creating a new table, populating it with fresh data, and swapping the relfilenumbers to preserve the original OID and permissions.

## Definition
```c
ObjectAddress RefreshMatViewByOid(Oid matviewOid, bool skipData, bool concurrent, const char *queryString, ParamListInfo params, QueryCompletion *qc)
```

## Detailed Description
This function implements the complete materialized view refresh mechanism using a create-and-swap strategy. It creates a transient table with the same structure as the materialized view, optionally populates it with fresh data from the view's underlying query, then swaps the storage files to make the new data visible while preserving the original relation OID, permissions, and references.

The function supports both concurrent and non-concurrent refresh modes:
- Concurrent refresh uses a temporary tablespace and performs a merge operation to minimize locking
- Non-concurrent refresh performs a direct heap swap with exclusive locking

Key validation steps include verifying the relation is a materialized view, checking for unique indexes when using concurrent mode, and ensuring no conflicting options are specified. The function also handles security context switching to run as the relation owner and manages transaction-level GUC changes.

## Parameters / Member Variables
- `matviewOid`: Object identifier of the materialized view to refresh
- `skipData`: Boolean flag indicating whether to skip data population (WITH NO DATA clause)
- `concurrent`: Boolean flag indicating whether to perform concurrent refresh
- `queryString`: Original SQL query string for logging and debugging
- `params`: ParamListInfo containing query parameters for the refresh operation
- `qc`: QueryCompletion structure for reporting processed row counts

## Dependencies
- Functions called/Symbols referenced:
  - table_open, table_close (relation access functions)
  - GetUserIdAndSecContext, SetUserIdAndSecContext (security context management)
  - NewGUCNestLevel, RestrictSearchPath (GUC and search path management)
  - RelationIsPopulated (checks if materialized view is populated)
  - RelationGetIndexList (retrieves list of indexes on relation)
  - index_open, index_close (index access functions)
  - is_usable_unique_index (validates unique index for concurrent refresh)
  - CheckTableNotInUse (ensures relation is not actively being used)
  - SetMatViewPopulatedState (updates materialized view populated flag)
  - make_new_heap (creates transient table with same structure)
  - CreateTransientRelDestReceiver (creates destination for query results)
  - refresh_matview_datafill (populates transient table with query data)
  - refresh_by_match_merge (concurrent refresh merge operation)
  - refresh_by_heap_swap (non-concurrent refresh swap operation)
  - pgstat_count_truncate, pgstat_count_heap_insert (statistics reporting)
  - AtEOXact_GUC (rollback GUC changes at transaction end)

- Called from (representative examples):
  - ExecRefreshMatView (main REFRESH MATERIALIZED VIEW command handler)
  - ExecCreateTableAs (when creating materialized views with initial data)

## Notes and Other Information
- The function preserves the original materialized view OID, maintaining all grants and references
- Concurrent refresh requires a usable unique index on the materialized view
- Security context is switched to the relation owner to execute functions with proper permissions
- The create-and-swap strategy allows for atomic replacement of materialized view contents
- Error handling includes proper cleanup of security context and GUC changes
- Row counts are tracked for pg_stat_statements but not displayed in command completion tags
- Indexes are rebuilt after data population for optimal performance (bulk loading followed by index creation)
- The function supports both populated and unpopulated final states based on the skipData parameter