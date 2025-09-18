# SetXidCommitTsInPage

## Location
src/backend/access/transam/commit_ts.c: 222 - 248

## Overview
Records commit timestamp entries for a main transaction and its subtransactions within a single SLRU page, ensuring atomic operation at the page level.

## Definition
```c
static void SetXidCommitTsInPage(TransactionId xid, int nsubxids,
                                TransactionId *subxids, TimestampTz ts,
                                RepOriginId nodeid, int64 pageno)
```

## Detailed Description
This static function handles the low-level details of writing commit timestamp data to a specific SLRU page. It ensures that all timestamp operations on a single page are atomic by acquiring the appropriate page lock, reading the page into memory if necessary, setting the timestamp data for both the main transaction and all subtransactions, marking the page as dirty, and then releasing the lock.

The function is designed to be called by TransactionTreeSetCommitTsData as part of its page-grouping optimization strategy. By operating on one page at a time, it minimizes lock contention and ensures data consistency within each page.

## Parameters / Member Variables
- `xid`: The main transaction ID to set timestamp for
- `nsubxids`: Number of subtransactions in the subxids array
- `subxids`: Array of subtransaction IDs (may be NULL if nsubxids is 0)
- `ts`: The commit timestamp to record
- `nodeid`: Replication origin ID for this commit
- `pageno`: The specific SLRU page number to operate on

## Dependencies
- Functions called/Symbols referenced:
  - SimpleLruGetBankLock (to get the appropriate lock for the page)
  - SimpleLruReadPage (to ensure the page is loaded into memory)
  - TransactionIdSetCommitTs (to set timestamp for individual transactions)
  - CommitTsCtl (global SLRU control structure for commit timestamps)
  - LWLock (lightweight lock type)
  - RepOriginId (replication origin identifier type)
- Called from (representative examples):
  - TransactionTreeSetCommitTsData

## Notes and Other Information
- This is a static function, only accessible within commit_ts.c
- Provides atomic operation guarantees at the page level through proper locking
- Marks the page as dirty after modifications to ensure persistence
- Uses the Simple LRU (SLRU) subsystem for efficient page management
- The function assumes all provided transaction IDs belong to the specified page
- Critical for maintaining ACID properties when recording commit timestamps
- Location: src/backend/access/transam/commit_ts.c:222-248