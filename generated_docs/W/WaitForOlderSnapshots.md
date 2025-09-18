# WaitForOlderSnapshots

## Location
src/backend/commands/indexcmds.c: 433 - 539

## Overview
Waits for transactions that might have an older snapshot than the given xmin limit, used when building an index concurrently to ensure data consistency.

## Definition
```c
void WaitForOlderSnapshots(TransactionId limitXmin, bool progress)
```

## Detailed Description
WaitForOlderSnapshots is a critical function used during concurrent index creation to ensure that no older transactions can see inconsistent data. The function identifies transactions that might have snapshots older than a specified xmin limit and waits for them to complete before proceeding.

The function works by obtaining a list of Virtual Transaction IDs (VXIDs) that represent transactions with potentially problematic snapshots. It then waits for each of these transactions individually by acquiring their virtual transaction locks.

The function applies several optimizations to exclude transactions that do not pose a consistency risk:
- Transactions with xmin > limitXmin (their snapshots are newer)
- Transactions with xmin = 0 (no live snapshot)
- Transactions in other databases (cannot see the index being built)
- Autovacuum processes and manual VACUUM operations
- CREATE INDEX CONCURRENTLY or REINDEX CONCURRENTLY processes on non-expressional, non-partial indexes

The function also implements dynamic rechecking - if a transaction goes idle or completes while waiting, it can be excluded from further waiting. This is achieved by repeatedly calling GetCurrentVirtualXIDs and comparing the results.

## Parameters / Member Variables
- `limitXmin`: The transaction ID limit - transactions with older snapshots need to be waited for
- `progress`: Boolean flag indicating whether to report progress information for monitoring

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentVirtualXIDs
  - VirtualTransactionIdIsValid
  - VirtualTransactionIdEquals
  - SetInvalidVirtualTransactionId
  - ProcNumberGetProc
  - VirtualXactLock
  - [pgstat_progress_update_param](../p/pgstat_progress_update_param.md)
- Called from (representative examples):
  - [DefineIndex](../D/DefineIndex.md)
  - [ATExecDetachPartitionFinalize](../A/ATExecDetachPartitionFinalize.md)

## Notes and Other Information
- Essential for maintaining MVCC consistency during concurrent index builds
- Uses progress reporting to allow monitoring of long-running operations
- Implements smart filtering to avoid waiting for transactions that cannot cause consistency issues
- The function never reports or waits for its own virtual transaction ID
- Dynamically adjusts the wait list as transactions complete or become idle
- Critical for the correctness of CREATE INDEX CONCURRENTLY operations
- Located in src/backend/commands/indexcmds.c:433-539