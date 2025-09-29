# SubTransGetParent

## Location
[src/backend/access/transam/subtrans.c:122-162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/subtrans.c#L122-L162)

## Overview
Retrieves the parent transaction ID of a given subtransaction from the subtransaction log, enabling traversal of the nested transaction hierarchy.

## Definition
```c
TransactionId
SubTransGetParent(TransactionId xid)
```

## Detailed Description
SubTransGetParent queries the SUBTRANS log to find the parent transaction ID of a specified subtransaction. This function is essential for PostgreSQL's nested transaction support, allowing the system to navigate up the transaction hierarchy. The function uses read-only access to the Simple LRU buffer system for efficient retrieval of subtransaction relationship data.

The function includes several important checks: it ensures the requested transaction ID is within the valid range (not older than TransactionXmin), handles special cases for bootstrap and frozen transaction IDs by returning InvalidTransactionId, and uses read-only page access to minimize locking overhead.

## Parameters / Member Variables
- `xid`: The subtransaction ID whose parent transaction ID is being queried

## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdToPage](../T/TransactionIdToPage.md) (converts transaction ID to page number)
  - TransactionIdToEntry (converts transaction ID to entry within page)
  - [TransactionIdFollowsOrEquals](../T/TransactionIdFollowsOrEquals.md) (validates transaction ID is not too old)
  - TransactionIdIsNormal (checks if transaction ID is a normal user transaction)
  - [SimpleLruReadPage_ReadOnly](SimpleLruReadPage_ReadOnly.md) (reads SLRU page in read-only mode)
  - [SimpleLruGetBankLock](SimpleLruGetBankLock.md) (gets lock for SLRU page)
  - SubTransCtl (global SLRU control structure for subtransactions)
- Called from (representative examples):
  - [SubTransGetTopmostTransaction](SubTransGetTopmostTransaction.md) (for traversing to root transaction)
  - [TransactionIdDidCommit](../T/TransactionIdDidCommit.md) (to check parent transaction status)
  - [TransactionIdDidAbort](../T/TransactionIdDidAbort.md) (to check parent transaction status)

## Notes and Other Information
- Uses read-only page access for better concurrency compared to exclusive locking
- Returns InvalidTransactionId for bootstrap and frozen transaction IDs, which have no parents
- Includes assertion to prevent queries for transactions that may have been truncated
- The function automatically handles locking and unlocking of the appropriate SLRU page
- Part of PostgreSQL's MVCC implementation for handling nested transaction status queries
- Essential for determining transaction visibility and status in multi-level nested transactions

## Simplified Source

```c
TransactionId SubTransGetParent(TransactionId xid)
{
    int64 pageno = TransactionIdToPage(xid);
    int entryno = TransactionIdToEntry(xid);
    int slotno;
    TransactionId *ptr;
    TransactionId parent;

    // Validate transaction ID is not too old
    Assert(TransactionIdFollowsOrEquals(xid, TransactionXmin));

    // Bootstrap and frozen XIDs have no parent
    if (!TransactionIdIsNormal(xid))
        return InvalidTransactionId;

    // Read the SLRU page containing this transaction's parent info
    slotno = SimpleLruReadPage_ReadOnly(SubTransCtl, pageno, xid);
    ptr = (TransactionId *) SubTransCtl->shared->page_buffer[slotno];
    ptr += entryno;

    parent = *ptr;

    // Release the lock on the SLRU page
    LWLockRelease(SimpleLruGetBankLock(SubTransCtl, pageno));

    return parent;
}
```