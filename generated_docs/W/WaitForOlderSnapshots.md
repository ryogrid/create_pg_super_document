# WaitForOlderSnapshots

## Location
[src/backend/commands/indexcmds.c:433-539](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/indexcmds.c#L433-L539)

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
  - [GetCurrentVirtualXIDs](../G/GetCurrentVirtualXIDs.md)
  - VirtualTransactionIdIsValid
  - VirtualTransactionIdEquals
  - SetInvalidVirtualTransactionId
  - [ProcNumberGetProc](../P/ProcNumberGetProc.md)
  - [VirtualXactLock](../V/VirtualXactLock.md)
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

## Simplified Source

```c
void WaitForOlderSnapshots(TransactionId limitXmin, bool progress) {
    int n_old_snapshots;
    VirtualTransactionId *old_snapshots;

    // Get list of transactions with snapshots older than limitXmin
    // Exclude autovacuum, vacuum, and safe concurrent index processes
    old_snapshots = GetCurrentVirtualXIDs(limitXmin, true, false,
                                         PROC_IS_AUTOVACUUM | PROC_IN_VACUUM | PROC_IN_SAFE_IC,
                                         &n_old_snapshots);

    // Report total number of snapshots to wait for
    if (progress)
        pgstat_progress_update_param(PROGRESS_WAITFOR_TOTAL, n_old_snapshots);

    // Wait for each old snapshot transaction
    for (int i = 0; i < n_old_snapshots; i++) {
        if (!VirtualTransactionIdIsValid(old_snapshots[i]))
            continue;  // Skip invalid transactions

        // Recheck if transactions are still active (optimization)
        if (i > 0) {
            VirtualTransactionId *newer_snapshots;
            int n_newer_snapshots;

            // Get updated list of active transactions
            newer_snapshots = GetCurrentVirtualXIDs(limitXmin, true, false,
                                                   PROC_IS_AUTOVACUUM | PROC_IN_VACUUM | PROC_IN_SAFE_IC,
                                                   &n_newer_snapshots);

            // Mark completed transactions as invalid
            for (int j = i; j < n_old_snapshots; j++) {
                if (!VirtualTransactionIdIsValid(old_snapshots[j]))
                    continue;

                bool still_active = false;
                for (int k = 0; k < n_newer_snapshots; k++) {
                    if (VirtualTransactionIdEquals(old_snapshots[j], newer_snapshots[k])) {
                        still_active = true;
                        break;
                    }
                }
                if (!still_active)
                    SetInvalidVirtualTransactionId(old_snapshots[j]);
            }
            pfree(newer_snapshots);
        }

        // Wait for this transaction if still valid
        if (VirtualTransactionIdIsValid(old_snapshots[i])) {
            // Report which process we're waiting for
            if (progress) {
                PGPROC *holder = ProcNumberGetProc(old_snapshots[i].procNumber);
                if (holder)
                    pgstat_progress_update_param(PROGRESS_WAITFOR_CURRENT_PID, holder->pid);
            }

            // Actually wait for the transaction
            VirtualXactLock(old_snapshots[i], true);
        }

        // Update progress counter
        if (progress)
            pgstat_progress_update_param(PROGRESS_WAITFOR_DONE, i + 1);
    }
}
```