# get_database_list

## Location
src/backend/postmaster/autovacuum.c: 1792 - 1876

## Overview
get_database_list retrieves a list of all valid databases from pg_database catalog, creating avw_dbase structures with essential database information for autovacuum processing.

## Definition
static List *get_database_list(void)

## Detailed Description
get_database_list is the only function in the autovacuum launcher that uses a transaction to access catalog data. It performs a sequential scan of the pg_database system catalog to build a comprehensive list of all databases that require autovacuum attention. The function carefully manages memory contexts to ensure that the returned data persists beyond the transaction scope while preventing memory leaks from intermediate operations.

The function extracts critical database metadata including the database OID, name, frozen XID, and minimum multixact ID, which are essential for determining vacuum priorities and freeze thresholds. It filters out invalid databases that are in the process of being dropped, ensuring that autovacuum doesn't waste resources on databases that no longer exist. The transaction management includes acquiring a snapshot primarily for its side effect of setting RecentGlobalXmin, which is crucial for HOT pruning safety during heap page reads.

## Parameters / Member Variables
This function takes no parameters and returns a List of avw_dbase structures.

## Dependencies
- Functions called/Symbols referenced:
  - StartTransactionCommand
  - GetTransactionSnapshot
  - table_open
  - table_beginscan_catalog
  - heap_getnext
  - table_endscan
  - table_close
  - CommitTransactionCommand
  - database_is_invalid_form
  - MemoryContextSwitchTo
  - palloc
  - pstrdup
  - lappend
  - elog
  - GETSTRUCT
  - NameStr
- Called from (representative examples):
  - rebuild_database_list
  - do_start_worker

## Notes and Other Information
The function includes a FIXME comment indicating a potential bug related to snapshot management and HOT pruning prevention. The comment suggests that an inactive snapshot may not reliably prevent HOT pruning due to possible xmin clearing during cache invalidation processing. The memory context switching is carefully orchestrated to allocate results in the caller's long-lived context while ensuring that temporary allocations from heap scanning occur in the shorter-lived transaction context. The function is specifically designed for the autovacuum launcher's unique requirement to access system catalogs without being connected to a specific database.