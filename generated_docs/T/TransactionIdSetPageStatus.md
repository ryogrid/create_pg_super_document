# TransactionIdSetPageStatus

## Location
[src/backend/access/transam/clog.c:293-363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/clog.c#L293-L363)

## Overview
Records the final state of transaction entries in the commit log for all transactions on a single CLOG page, with optimization for group updates to reduce lock contention.

## Definition

```c
static void
TransactionIdSetPageStatus(TransactionId xid, int nsubxids,
						   TransactionId *subxids, XidStatus status,
						   XLogRecPtr lsn, int64 pageno,
						   bool all_xact_same_page)
```
## Detailed Description
This function provides an optimized interface for updating transaction status entries on a single CLOG page. It implements a sophisticated group update mechanism to reduce SLRU bank lock contention when multiple backends are trying to update transaction status simultaneously.

The function has two main execution paths:

1. **Group Update Path**: When conditions are favorable (same-page transactions, matching cached XIDs, reasonable subtransaction count), it attempts to use a group update mechanism where one leader process performs updates for multiple backends, reducing lock contention.

2. **Direct Update Path**: When group update conditions aren't met or group update fails, it falls back to directly acquiring the SLRU bank lock and performing the update.

The group update optimization is particularly beneficial in high-concurrency scenarios where many transactions are committing simultaneously, as it reduces the frequency of lock acquisition and improves overall throughput.

## Parameters / Member Variables
- : Main transaction ID to update (can be InvalidTransactionId when only updating subtransactions)
- : Number of subtransaction IDs in the subxids array  
- : Array of subtransaction IDs to update on this page
- : The XidStatus to set for all transactions
- : WAL log sequence number for this status change
- : The CLOG page number where all these transactions reside
- : Boolean indicating whether all transactions are confirmed to be on the same page (enables optimizations)

## Dependencies
- Functions called/Symbols referenced:
  - [SimpleLruGetBankLock](../S/SimpleLruGetBankLock.md)
  - [LWLockConditionalAcquire](../L/LWLockConditionalAcquire.md)
  - [TransactionIdSetPageStatusInternal](TransactionIdSetPageStatusInternal.md)
  - [TransactionGroupUpdateXidStatus](TransactionGroupUpdateXidStatus.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - XactCtl
  - THRESHOLD_SUBTRANS_CLOG_OPT
  - PGPROC_MAX_CACHED_SUBXIDS
- Called from (representative examples):
  - [TransactionIdSetTreeStatus](TransactionIdSetTreeStatus.md)
  - [set_status_by_pages](../s/set_status_by_pages.md)

## Notes and Other Information
- This is a static function intended for internal use within the CLOG subsystem
- Includes a compile-time assertion to ensure group update threshold doesn't exceed PGPROC cache limits
- The group update optimization requires that the XID and subxids match exactly with what's cached in MyProc
- Falls back gracefully to direct lock acquisition if group update mechanisms fail
- Optimizes for the common case where all transactions being updated are on the same CLOG page
- Part of PostgreSQL's broader effort to reduce lock contention in high-concurrency transaction processing scenarios