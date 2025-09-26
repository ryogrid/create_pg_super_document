# SerialAdd

## Location
src/backend/storage/lmgr/predicate.c: 858 - 948

## Overview
Records a committed read-write serializable transaction ID and its minimum conflict commit sequence number in the pg_serial SLRU system.

## Definition
```c
static void SerialAdd(TransactionId xid, SerCommitSeqNo minConflictCommitSeqNo)
```

## Detailed Description
This function is responsible for persistently storing information about committed serializable transactions that had read-write conflicts. It manages the pg_serial SLRU (Simple Least Recently Used) buffer system to track transaction commit sequence numbers for serializable isolation level conflict detection.

The function performs several key operations:
1. **Validation**: Checks if the transaction ID is still relevant (not older than the global xmin/tailXid)
2. **SLRU Management**: Determines the target page and manages SLRU buffer allocation
3. **Page Initialization**: Zeros out new pages entering the active range from tailXid to headXid
4. **Concurrent Control**: Uses both SerialControlLock and SLRU bank locks for thread safety
5. **Data Storage**: Records the minimum conflict commit sequence number for the transaction

The function handles two main scenarios:
- **Cold Start**: When the SLRU is currently unused, it zeros out the entire active region
- **Normal Operation**: Only zeros out new pages that enter the tailXid-headXid range

## Parameters / Member Variables
- `xid`: The transaction ID of the committed read-write serializable transaction
- `minConflictCommitSeqNo`: The minimum commit sequence number of any transactions to which this transaction had a read-write conflict out (InvalidSerCommitSeqNo if no conflicts)

## Dependencies
- Functions called/Symbols referenced:
  - `TransactionIdIsValid`
  - `TransactionIdPrecedes`
  - `TransactionIdFollows`
  - `SerialPage`
  - `SerialNextPage`
  - `SerialPagePrecedesLogically`
  - `SerialValue`
  - `SimpleLruGetBankLock`
  - `SimpleLruZeroPage`
  - `SimpleLruReadPage`
  - `LWLockAcquire`/`LWLockRelease`
- Called from (representative examples):
  - `SummarizeOldestCommittedSxact`

## Notes and Other Information
- Critical for maintaining serializable isolation level guarantees in PostgreSQL
- Uses sophisticated locking protocol: acquires SerialControlLock first, then individual SLRU bank locks
- Optimizes by early return if the transaction is older than the global xmin (no longer needed)
- Handles SLRU page initialization efficiently by zeroing ranges of pages when needed
- The function can involve "trading locks" when initializing multiple intervening pages
- Marks SLRU pages as dirty after writing to ensure proper persistence
- An invalid minConflictCommitSeqNo indicates the transaction had no read-write conflicts out
- Essential component of PostgreSQL's Serializable Snapshot Isolation (SSI) implementation