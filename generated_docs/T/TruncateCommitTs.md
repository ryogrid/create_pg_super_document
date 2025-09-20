# TruncateCommitTs

## Location
[src/backend/access/transam/commit_ts.c:890-915](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/commit_ts.c#L890-L915)

## Overview
Removes all commit timestamp SLRU segments before the segment containing a specified oldest transaction ID to reclaim storage space.

## Definition

```c
void
TruncateCommitTs(TransactionId oldestXact)
```
## Detailed Description
TruncateCommitTs is responsible for cleaning up old commit timestamp data by removing SLRU segments that are no longer needed. This function is typically called as part of vacuum operations to reclaim disk space used by commit timestamp data for transactions that are no longer of interest.

The function operates by:
1. Calculating the cutoff page based on the oldest transaction ID that needs to be preserved
2. Scanning the directory to check if there are actually files that can be removed
3. Writing a WAL record to ensure the truncation is properly logged for recovery
4. Performing the actual truncation using SimpleLruTruncate

The implementation is efficient in that it first checks whether any files can actually be removed before proceeding with the more expensive operations.

## Parameters / Member Variables
- : The oldest transaction ID that needs to be preserved; all commit timestamp data for older transactions will be removed

## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdToCTsPage](TransactionIdToCTsPage.md) (oldestXact)
  - [SlruScanDirectory](../S/SlruScanDirectory.md) (CommitTsCtl, SlruScanDirCbReportPresence, &cutoffPage)
  - [SlruScanDirCbReportPresence](../S/SlruScanDirCbReportPresence.md) (callback function)
  - [WriteTruncateXlogRec](../W/WriteTruncateXlogRec.md) (cutoffPage, oldestXact)
  - [SimpleLruTruncate](../S/SimpleLruTruncate.md) (CommitTsCtl, cutoffPage)
  - CommitTsCtl (SLRU control structure)

- Called from (representative examples):
  - [vac_truncate_clog](../v/vac_truncate_clog.md) (vacuum cleanup function)

## Notes and Other Information
- This function does not need to flush WAL since the truncation is logged via WriteTruncateXlogRec
- The function is optimized to early-return if no files can be removed
- The cutoff point is the start of the segment containing oldestXact, not the transaction itself
- WAL logging ensures that the truncation is properly handled during recovery
- This is typically called during vacuum operations to reclaim space from old commit timestamp data
- The function is exported via commit_ts.h for use by vacuum and other cleanup processes