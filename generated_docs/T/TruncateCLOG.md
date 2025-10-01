# TruncateCLOG

## Location
[src/backend/access/transam/clog.c:1000-1054](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/clog.c#L1000-L1054)

## Overview
TruncateCLOG removes old CLOG segments before a specified transaction ID, reclaiming disk space while ensuring proper WAL logging and crash safety.

## Definition
```c
void TruncateCLOG(TransactionId oldestXact, Oid oldestxid_datoid)
```

## Detailed Description
TruncateCLOG removes all CLOG (Commit Log) segments that contain only transaction IDs older than the specified oldestXact. The function implements several important safety measures to ensure crash consistency and proper operation of standby servers.

Before removing any CLOG data, the function flushes XLOG to disk to ensure that any recently-emitted FREEZE_PAGE records have been persisted. This prevents crashes from leaving unfrozen tuples that reference removed CLOG data. The function also generates a special TRUNCATE XLOG record to help prevent hot standby servers from maintaining unreasonably bloated CLOG directories.

Since CLOG segments contain many transactions, actual removal opportunities are rare. The function optimizes by only performing the XLOG flush after confirming that removable segments exist. It also advances the oldestClogXid before truncation to ensure concurrent transaction status lookups don't attempt to access truncated data.

## Parameters / Member Variables
- `oldestXact`: The oldest transaction ID that should be kept; all CLOG data for older transactions will be removed
- `oldestxid_datoid`: The database OID associated with the oldest transaction ID

## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdToPage](TransactionIdToPage.md)
  - [SlruScanDirectory](../S/SlruScanDirectory.md)
  - [SlruScanDirCbReportPresence](../S/SlruScanDirCbReportPresence.md)
  - [AdvanceOldestClogXid](../A/AdvanceOldestClogXid.md)
  - [WriteTruncateXlogRec](../W/WriteTruncateXlogRec.md)
  - [SimpleLruTruncate](../S/SimpleLruTruncate.md)
- Global variables accessed:
  - XactCtl
- Called from:
  - [vac_truncate_clog](../v/vac_truncate_clog.md) (src/backend/commands/vacuum.c:1936)

## Notes and Other Information
- Only performs truncation if removable segments are found via SlruScanDirectory
- Ensures crash safety by flushing XLOG before removing CLOG data
- Advances oldestClogXid before truncation to prevent concurrent access to truncated data
- Generates WAL records to help standby servers maintain reasonable CLOG directory sizes
- The cutoff point is the start of the segment containing oldestXact
- Removal opportunities are rare since CLOG segments hold many transactions
- Essential for preventing unlimited CLOG growth in long-running PostgreSQL instances

## Simplified Source

```c
void
TruncateCLOG(TransactionId oldestXact, Oid oldestxid_datoid)
{
    int64 cutoffPage;

    // Calculate the page containing the oldest transaction to keep
    cutoffPage = TransactionIdToPage(oldestXact);

    // Check if there are any removable CLOG files
    if (!SlruScanDirectory(XactCtl, SlruScanDirCbReportPresence, &cutoffPage))
        return; // Nothing to remove

    // Advance the oldest CLOG XID before truncation to prevent
    // concurrent lookups from accessing truncated data
    AdvanceOldestClogXid(oldestXact);

    // Write WAL record and flush to ensure crash safety
    // This prevents unfrozen tuples from referencing removed CLOG data
    WriteTruncateXlogRec(cutoffPage, oldestXact, oldestxid_datoid);

    // Now safe to remove the old CLOG segments
    SimpleLruTruncate(XactCtl, cutoffPage);
}
```