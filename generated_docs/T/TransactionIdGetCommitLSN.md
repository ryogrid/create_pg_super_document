# TransactionIdGetCommitLSN

## Location
[src/backend/access/transam/transam.c:382-405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/transam.c#L382-L405)

## Overview
TransactionIdGetCommitLSN returns a WAL Log Sequence Number (LSN) that guarantees the transaction's commit record has been flushed to disk when WAL is flushed up to that LSN.

## Definition
```c
XLogRecPtr TransactionIdGetCommitLSN(TransactionId xid)
```

## Detailed Description
This function provides an LSN that can be used to ensure durability of a transaction's commit. When WAL is flushed up to the returned LSN, the transaction's commit record is guaranteed to be on disk. However, the returned LSN is not necessarily the exact LSN of the transaction's commit record.

The function implements several optimizations and special cases:
- Uses a cache (cachedFetchXid/cachedCommitLSN) to avoid repeated shared memory access for recently queried transactions
- Returns InvalidXLogRecPtr for special transaction IDs and long-past transactions whose clog pages have migrated to disk
- May return the LSN of a later transaction due to clog page grouping optimizations

The function is primarily used in visibility checking contexts where the system needs to determine if a transaction's commit has been durably recorded.

## Parameters / Member Variables
- `xid`: The transaction ID for which to retrieve the commit LSN

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdEquals (compares transaction IDs for equality)
  - TransactionIdIsNormal (checks if transaction ID is a normal user transaction)
  - [TransactionIdGetStatus](TransactionIdGetStatus.md) (retrieves transaction status and associated LSN)
- Called from (representative examples):
  - [SetHintBits](../S/SetHintBits.md) (heap tuple visibility hint bit setting)

## Notes and Other Information
The function leverages caching from TransactionLogFetch operations since most callers have recently checked transaction status. For special XIDs (bootstrap, frozen, etc.), it returns InvalidXLogRecPtr since these are always considered committed without needing WAL records. The grouping of transactions on the same clog page means the returned LSN might correspond to a later transaction in the same group, which still provides the necessary durability guarantee.

## Simplified Source

```c
XLogRecPtr
TransactionIdGetCommitLSN(TransactionId xid)
{
    XLogRecPtr result;

    // Check cache first (optimization for recently fetched transactions)
    if (TransactionIdEquals(xid, cachedFetchXid))
        return cachedCommitLSN;

    // Special XIDs are always committed, no WAL record needed
    if (!TransactionIdIsNormal(xid))
        return InvalidXLogRecPtr;

    // Get transaction status and associated LSN
    (void) TransactionIdGetStatus(xid, &result);

    return result;
}
```