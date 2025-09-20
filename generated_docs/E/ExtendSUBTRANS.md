# ExtendSUBTRANS

## Location
[src/backend/access/transam/subtrans.c:379-410](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/subtrans.c#L379-L410)

## Overview
Ensures that the SUBTRANS system has room for a newly-allocated transaction ID by extending and zeroing new pages as needed during transaction ID allocation.

## Definition

```c
void
ExtendSUBTRANS(TransactionId newestXact)
```
## Detailed Description
ExtendSUBTRANS is called during transaction ID allocation to ensure that the SUBTRANS system can accommodate the newest transaction. It's designed to be very fast most of the time by only doing work when a new page needs to be allocated.

The function only performs actual work when the transaction ID is the first entry of a new page, with special handling for the wraparound case where the first XID of page zero is FirstNormalTransactionId. When extension is needed, it acquires the appropriate bank lock, zeros the new page using ZeroSUBTRANSPage, and releases the lock.

This function is called while holding XidGenLock, so it's optimized for speed. Even when it needs to do work, no actual I/O occurs unless a dirty SUBTRANS page needs to be written out to make room in shared memory.

## Parameters / Member Variables
- : The newest transaction ID that has been allocated and needs SUBTRANS space

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdToEntry
  - TransactionIdEquals
  - [TransactionIdToPage](../T/TransactionIdToPage.md)
  - [SimpleLruGetBankLock](../S/SimpleLruGetBankLock.md)
  - ZeroSUBTRANSPage
  - LWLockAcquire/LWLockRelease
  - FirstNormalTransactionId
- Called from (representative examples):
  - GetNewTransactionId (during normal transaction allocation)
  - ProcArrayApplyRecoveryInfo (during recovery)
  - [RecordKnownAssignedTransactionIds](../R/RecordKnownAssignedTransactionIds.md) (during recovery)

## Notes and Other Information
- Called while holding XidGenLock, so must be fast
- Only does work at the first XID of a new page
- Handles wraparound case specially for FirstNormalTransactionId
- Uses bank locking for efficient concurrent access
- No I/O required unless shared memory needs to be freed
- Critical for maintaining SUBTRANS page availability during high transaction throughput