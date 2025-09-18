# ExecRefreshMatView

## Location
[src/backend/commands/matview.c:121-161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/matview.c#L121-L161)

## Overview
ExecRefreshMatView executes a REFRESH MATERIALIZED VIEW SQL command, handling both WITH DATA and WITH NO DATA variants with support for concurrent refreshing.

## Definition
```c
ObjectAddress ExecRefreshMatView(RefreshMatViewStmt *stmt, const char *queryString, ParamListInfo params, QueryCompletion *qc)
```

## Detailed Description
This function serves as the main entry point for executing REFRESH MATERIALIZED VIEW commands. It determines the appropriate lock mode based on whether concurrent refresh was requested, acquires the necessary lock on the materialized view relation, and delegates the actual refresh work to RefreshMatViewByOid.

The function handles two primary scenarios:
1. WITH NO DATA: Effectively truncates the materialized view, leaving it empty
2. WITH DATA (default): Truncates and repopulates the view using its underlying SELECT query

For concurrent refresh operations, it uses ExclusiveLock to allow other transactions to read the view during refresh. For non-concurrent refresh, it uses AccessExclusiveLock to prevent all access during the operation.

## Parameters / Member Variables
- `stmt`: RefreshMatViewStmt structure containing the parsed REFRESH MATERIALIZED VIEW statement with flags for skipData and concurrent options
- `queryString`: The original SQL query string for logging and debugging purposes  
- `params`: ParamListInfo containing any parameters for the refresh operation
- `qc`: QueryCompletion structure for reporting completion statistics

## Dependencies
- Functions called/Symbols referenced:
  - RefreshMatViewStmt (statement node structure)
  - [ParamListInfo](../P/ParamListInfo.md) (query parameters structure)
  - QueryCompletion (completion reporting structure)
  - ExclusiveLock (lock mode constant for concurrent refresh)
  - AccessExclusiveLock (lock mode constant for non-concurrent refresh)
  - [RangeVarGetRelidExtended](../R/RangeVarGetRelidExtended.md) (resolves relation name to OID with locking)
  - [RangeVarCallbackMaintainsTable](../R/RangeVarCallbackMaintainsTable.md) (callback for relation validation)
  - [RefreshMatViewByOid](../R/RefreshMatViewByOid.md) (performs the actual refresh work)

- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (utility command processing in tcop/utility.c)

## Notes and Other Information
- Lock mode selection is critical: concurrent refresh uses ExclusiveLock to allow reads, while non-concurrent uses AccessExclusiveLock for exclusive access
- The function uses RangeVarCallbackMaintainsTable to validate that the target relation can be maintained (is a materialized view)
- This is a thin wrapper that primarily handles locking strategy and delegates actual work to RefreshMatViewByOid
- The skipData field in the statement determines whether to repopulate the view or just truncate it
- Returns an ObjectAddress identifying the refreshed materialized view