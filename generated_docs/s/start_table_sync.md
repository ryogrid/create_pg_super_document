# start_table_sync

## Location
src/backend/replication/logical/tablesync.c: 1669 - 1710

## Overview
start_table_sync provides a robust error-handling wrapper around table synchronization operations, implementing subscription disabling on failure and proper memory management for slot names.

## Definition
```c
static void start_table_sync(XLogRecPtr *origin_startpos, char **slotname)
```

## Detailed Description
This function serves as a protective wrapper around the core table synchronization functionality, implementing comprehensive error handling and resource management strategies. It encapsulates the potentially failure-prone table synchronization process within PostgreSQL's exception handling framework (PG_TRY/PG_CATCH).

The function implements two distinct error handling strategies based on subscription configuration:

1. **Automatic Subscription Disabling**: When the subscription is configured with 'disableonerr', any error during synchronization triggers automatic subscription disabling and worker exit
2. **Error Reporting and Re-throwing**: For normal subscriptions, errors are reported to the statistics system and re-thrown for upstream handling

The function also manages memory allocation carefully, ensuring that the returned slot name is allocated in the long-lived ApplyContext to survive beyond the current function scope, while properly cleaning up the temporary allocation from LogicalRepSyncTableStart.

## Parameters / Member Variables
- `origin_startpos`: Output parameter that receives the LSN position where logical replication should begin after initial synchronization
- `slotname`: Output parameter that receives the allocated slot name for the table sync operation, allocated in ApplyContext

## Dependencies
- Functions called/Symbols referenced:
  - am_tablesync_worker
  - LogicalRepSyncTableStart
  - DisableSubscriptionAndExit
  - AbortOutOfAnyTransaction
  - pgstat_report_subscription_error
  - MemoryContextStrdup
- Called from (representative examples):
  - run_tablesync_worker

## Notes and Other Information
- Uses PostgreSQL's PG_TRY/PG_CATCH/PG_END_TRY exception handling framework for robust error management
- The function specifically excludes handling of FATAL errors, which are assumed to be system resource errors that are not recoverable
- Memory management ensures the slot name survives in ApplyContext while cleaning up temporary allocations
- The 'disableonerr' feature provides a fail-safe mechanism for subscriptions in production environments where continued failures should result in automatic disabling
- Error reporting integration with PostgreSQL's statistics system ensures proper monitoring and diagnostics
- Assert statement ensures this function is only called by table synchronization workers
- The function abstracts the complexity of error handling from the core synchronization logic in LogicalRepSyncTableStart