# TransactionIdGetStatus

## Location
[src/backend/access/transam/clog.c:735-767](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/clog.c#L735-L767)

## Overview
A low-level function that queries the commit status of a transaction from the CLOG and returns the associated LSN for flush guarantees.

## Definition

```c
XidStatus
TransactionIdGetStatus(TransactionId xid, XLogRecPtr *lsn)
```
## Detailed Description
TransactionIdGetStatus is the fundamental function for reading transaction status from the Commit Log (CLOG). It performs bit-level operations to extract the 2-bit status value for a specific transaction ID from the CLOG buffer pages.

Beyond just returning the commit status, this function also provides an important service for WAL flushing: it returns an LSN that guarantees if WAL is flushed up to that point, the transaction's commit record will be on disk. This LSN might not be the exact LSN of the transaction's commit record - it could be from a later transaction in the same group, or InvalidXLogRecPtr for very old transactions whose CLOG pages have been flushed to disk.

The function uses SimpleLruReadPage_ReadOnly to access the CLOG page, which handles the necessary locking automatically. It calculates the exact byte and bit position for the transaction, extracts the status bits, and retrieves the corresponding group LSN.

## Parameters
- : The transaction ID to look up
- : Output parameter - receives an LSN suitable for flush guarantee purposes

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdToPage, TransactionIdToByte, TransactionIdToBIndex
  - SimpleLruReadPage_ReadOnly, SimpleLruGetBankLock
  - GetLSNIndex
  - CLOG_BITS_PER_XACT, CLOG_XACT_BITMASK
- Called from:
  - TransactionLogFetch
  - TransactionIdGetCommitLSN

## Notes and Other Information
- This is a low-level routine - TransactionLogFetch() in transam.c is the preferred high-level interface
- The returned LSN may be from a later transaction in the same group for storage efficiency
- For very old transactions, InvalidXLogRecPtr may be returned as the LSN
- Lock acquisition and release is handled automatically by SimpleLruReadPage_ReadOnly
- The function extracts exactly 2 bits per transaction from the packed CLOG page format
- Group LSN tracking enables efficient batch flushing of related transactions