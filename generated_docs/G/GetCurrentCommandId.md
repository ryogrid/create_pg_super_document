# GetCurrentCommandId

## Location
src/backend/access/transam/xact.c: 826 - 855

## Overview
Returns the current command ID within the active transaction, with special handling for parallel workers and usage tracking.

## Definition


## Detailed Description
GetCurrentCommandId returns the current command ID (currentCommandId) for the active transaction. The function includes important logic for tracking command ID usage and enforcing restrictions in parallel worker contexts. When the 'used' parameter is true, it indicates the caller intends to modify data (insert/update/delete tuples), which triggers usage tracking by setting currentCommandIdUsed to true. However, this is forbidden in parallel workers since there's no mechanism to communicate this state back to the leader process. When 'used' is false, the ID is being fetched for read-only purposes such as snapshot validity checks.

## Parameters / Member Variables
- : Boolean flag indicating whether the command ID will be used for data modifications (true) or read-only purposes (false)

## Dependencies
- Functions called/Symbols referenced:
  - IsParallelWorker (function to check if running in parallel worker)
  - ereport (error reporting function)
  - currentCommandId (global variable)
  - currentCommandIdUsed (global variable)
- Called from (representative examples):
  - toast_save_datum (src/backend/access/common/toast_internals.c:128)
  - simple_heap_insert (src/backend/access/heap/heapam.c:2675)
  - simple_heap_delete (src/backend/access/heap/heapam.c:3160)
  - simple_heap_update (src/backend/access/heap/heapam.c:4452)
  - CopyFrom (src/backend/commands/copyfrom.c:641)
  - standard_ExecutorStart (src/backend/executor/execMain.c:216,232)
  - GetSnapshotData (src/backend/storage/ipc/procarray.c:2510)

## Notes and Other Information
- Command IDs are global to a transaction, not subtransaction-local
- Parallel workers cannot modify data and will throw an error if 'used' is true
- The function is critical for MVCC (Multi-Version Concurrency Control) implementation
- Located in src/backend/access/transam/xact.c:826-855
- Used extensively throughout the system for both data modification and snapshot management