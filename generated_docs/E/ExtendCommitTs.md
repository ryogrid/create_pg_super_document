# ExtendCommitTs

## Location
[src/backend/access/transam/commit_ts.c:849-889](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/commit_ts.c#L849-L889)

## Overview
Ensures the commit timestamp SLRU has sufficient space for a newly-allocated transaction ID by extending storage when needed.

## Definition

```c
void
ExtendCommitTs(TransactionId newestXact)
```
## Detailed Description
ExtendCommitTs is called to ensure that the commit timestamp SLRU has room to accommodate a newly-allocated transaction ID. This function is designed to be very fast in the common case and is called while holding XidGenLock, so performance is critical.

The function operates efficiently by:
1. Early return if commit timestamp tracking is disabled
2. Only performing work when the transaction ID is the first on a new page
3. Special handling for wraparound scenarios where the first XID of page zero is FirstNormalTransactionId
4. Using bank-specific locking to minimize contention
5. Zeroing the new page and creating appropriate WAL records

The implementation assumes track_commit_timestamp is a PGC_POSTMASTER parameter, meaning it can only be changed at server startup.

## Parameters / Member Variables
- : The newly allocated transaction ID that needs storage space in the commit timestamp SLRU

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdToCTsEntry (newestXact)
  - TransactionIdEquals (newestXact, FirstNormalTransactionId)
  - [TransactionIdToCTsPage](../T/TransactionIdToCTsPage.md) (newestXact)
  - [SimpleLruGetBankLock](../S/SimpleLruGetBankLock.md) (CommitTsCtl, pageno)
  - LWLockAcquire (lock, LW_EXCLUSIVE)
  - [ZeroCommitTsPage](../Z/ZeroCommitTsPage.md) (pageno, !InRecovery)
  - LWLockRelease (lock)
  - Assert (!InRecovery)
  - FirstNormalTransactionId (constant)
  - CommitTsCtl (SLRU control structure)

- Called from (representative examples):
  - GetNewTransactionId (main transaction ID allocation function)

## Notes and Other Information
- This function is only called from GetNewTransactionId, which never runs on standby servers
- Performance is critical as it's called while holding XidGenLock
- Uses unlocked read of commitTsActive flag, which is safe in this context
- Only does actual work at the first XID of each SLRU page
- Handles transaction ID wraparound correctly by special-casing FirstNormalTransactionId
- Uses bank-specific locking rather than global locks to improve concurrency
- Creates WAL records for the page zeroing operation when not in recovery mode
- The function assumes commit timestamp tracking cannot be toggled during runtime