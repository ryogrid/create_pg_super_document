# TransactionIdToCTsPage

## Location
[src/backend/access/transam/commit_ts.c:72-76](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/commit_ts.c#L72-L76)

## Overview
Calculates the page number within the commit timestamp SLRU that contains the commit timestamp entry for a given transaction ID.

## Definition


## Detailed Description
This function performs a simple mathematical calculation to determine which page in the commit timestamp Simple LRU (SLRU) buffer contains the commit timestamp data for a specified transaction ID. The function divides the transaction ID by the number of transaction entries that can fit in a single page (COMMIT_TS_XACTS_PER_PAGE) to obtain the page number. This is a core utility function used throughout the commit timestamp subsystem for page-based access to commit timestamp data.

The function returns an int64 value, though the comment notes that the actual value currently cannot exceed 0xFFFFFFFF/COMMIT_TS_XACTS_PER_PAGE due to implementation constraints.

## Parameters / Member Variables
- `xid`: The transaction ID for which to find the containing page number

## Dependencies
- Functions called/Symbols referenced:
  - COMMIT_TS_XACTS_PER_PAGE (macro that calculates transactions per page as BLCKSZ / SizeOfCommitTimestampEntry)
- Called from (representative examples):
  - [TransactionTreeSetCommitTsData](TransactionTreeSetCommitTsData.md)
  - [TransactionIdGetCommitTsData](TransactionIdGetCommitTsData.md)
  - [ActivateCommitTs](../A/ActivateCommitTs.md)
  - [ExtendCommitTs](../E/ExtendCommitTs.md)
  - [TruncateCommitTs](TruncateCommitTs.md)

## Notes and Other Information
- This is a static inline function, meaning it's optimized for performance and only accessible within the commit_ts.c file
- The page calculation is essential for the SLRU-based storage system used for commit timestamps
- The function assumes that transaction IDs are distributed sequentially across pages
- Location: src/backend/access/transam/commit_ts.c:72-76