# get_subscription_list

## Location
src/backend/replication/logical/launcher.c: 112 - 182

## Overview
Retrieves a list of all active logical replication subscriptions from the pg_subscription catalog, filtering for fields relevant to worker start/stop operations.

## Definition

```c
static List *
get_subscription_list(void)
```
## Detailed Description
The get_subscription_list function scans the pg_subscription system catalog to build a list of all subscriptions in the database. It extracts essential subscription information needed by the logical replication launcher to manage worker processes. The function operates within its own transaction context to ensure consistent reads from the catalog while carefully managing memory allocation to prevent leaks.

The function uses a heap scan over the pg_subscription table and creates Subscription structures containing only the fields necessary for worker management: oid, database id, owner, enabled status, and name. Memory allocation is performed in the caller's context rather than the transaction context to ensure the results persist beyond the transaction's lifetime.

## Parameters / Member Variables
- Returns:  - A list of Subscription structures containing essential subscription information

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
  - MemoryContextSwitchTo
  - palloc0
  - pstrdup
  - lappend
- Called from:
  - ApplyLauncherMain

## Notes and Other Information
- The function includes a FIXME comment noting that the snapshot handling may not reliably prevent HOT pruning as intended
- Memory context switching is used within the scan loop to allocate results in the caller's context while preventing leaks from heap operations
- Only fills subscription fields relevant to worker start/stop operations, leaving other fields uninitialized for efficiency
- Operates under AccessShareLock on the pg_subscription relation to allow concurrent reads