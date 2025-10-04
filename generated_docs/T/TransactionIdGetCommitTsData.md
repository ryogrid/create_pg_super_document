# TransactionIdGetCommitTsData

## Location
[src/backend/access/transam/commit_ts.c:274-359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/commit_ts.c#L274-L359)

## Overview
Retrieves the commit timestamp and replication origin information for a given transaction ID, with optimizations for cached values and comprehensive validation.

## Definition
```c
bool TransactionIdGetCommitTsData(TransactionId xid, TimestampTz *ts,
                                 RepOriginId *nodeid)
```

## Detailed Description
This function provides the primary interface for querying commit timestamp information. It implements several optimization strategies and validation checks:

1. **Input validation**: Checks if the transaction ID is valid and normal
2. **Cached value optimization**: Returns immediately if the requested XID matches the cached last commit
3. **Range validation**: Ensures the requested XID falls within the valid commit timestamp range
4. **SLRU access**: Reads from the appropriate SLRU page when cached data is not available

The function handles special cases like bootstrap and frozen XIDs by returning a timestamp of 0, and provides comprehensive error handling for invalid inputs and disabled commit timestamp tracking.

## Parameters / Member Variables
- `xid`: The transaction ID to query for commit timestamp information
- `ts`: Output parameter for the commit timestamp (must not be NULL)
- `nodeid`: Output parameter for replication origin ID (may be NULL if not needed)

## Return Value
- `true`: Valid commit timestamp data was found and returned
- `false`: No commit timestamp data available (returns 0 for timestamp)

## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdToCTsPage](TransactionIdToCTsPage.md) (to determine the SLRU page)
  - TransactionIdToCTsEntry (to calculate entry offset within page)
  - TransactionIdIsValid/TransactionIdIsNormal (validation functions)
  - [TransactionIdPrecedes](TransactionIdPrecedes.md) (for range checking)
  - [error_commit_ts_disabled](../e/error_commit_ts_disabled.md) (error reporting when feature disabled)
  - [SimpleLruReadPage_ReadOnly](../S/SimpleLruReadPage_ReadOnly.md) (to read SLRU page)
  - [SimpleLruGetBankLock](../S/SimpleLruGetBankLock.md) (to get page lock for release)
  - [CommitTimestampEntry](../C/CommitTimestampEntry.md) (structure for timestamp data)
- Called from (representative examples):
  - [pg_xact_commit_timestamp](../p/pg_xact_commit_timestamp.md) (SQL function interface)
  - [pg_xact_commit_timestamp_origin](../p/pg_xact_commit_timestamp_origin.md) (SQL function interface)

## Notes and Other Information
- Uses shared lock (LW_SHARED) for reading commit timestamp shared state
- Implements caching optimization for recently committed transactions
- Handles bootstrap and frozen XIDs as special cases (always return false)
- Validates that requested XID is within the valid commit timestamp range
- Uses read-only SLRU access when reading from disk storage
- Returns false (with timestamp 0) when no valid timestamp data exists
- Critical function for commit timestamp queries and replication scenarios
- Location: src/backend/access/transam/commit_ts.c:274-359

## Simplified Source

```c
bool TransactionIdGetCommitTsData(TransactionId xid, TimestampTz *ts, RepOriginId *nodeid)
{
    int64 pageno = TransactionIdToCTsPage(xid);
    int entryno = TransactionIdToCTsEntry(xid);
    CommitTimestampEntry entry;

    // Validate input transaction ID
    if (!TransactionIdIsValid(xid))
        ereport(ERROR, (errmsg("cannot retrieve commit timestamp for transaction %u", xid)));

    if (!TransactionIdIsNormal(xid))
    {
        // Bootstrap and frozen XIDs have no commit timestamp
        *ts = 0;
        if (nodeid) *nodeid = 0;
        return false;
    }

    LWLockAcquire(CommitTsLock, LW_SHARED);

    // Check if commit timestamp tracking is enabled
    if (!commitTsShared->commitTsActive)
        error_commit_ts_disabled();

    // Return cached value if available
    if (commitTsShared->xidLastCommit == xid)
    {
        *ts = commitTsShared->dataLastCommit.time;
        if (nodeid) *nodeid = commitTsShared->dataLastCommit.nodeid;
        LWLockRelease(CommitTsLock);
        return *ts != 0;
    }

    // Check if XID is within valid range
    TransactionId oldestCommitTsXid = TransamVariables->oldestCommitTsXid;
    TransactionId newestCommitTsXid = TransamVariables->newestCommitTsXid;
    LWLockRelease(CommitTsLock);

    if (!TransactionIdIsValid(oldestCommitTsXid) ||
        TransactionIdPrecedes(xid, oldestCommitTsXid) ||
        TransactionIdPrecedes(newestCommitTsXid, xid))
    {
        *ts = 0;
        if (nodeid) *nodeid = InvalidRepOriginId;
        return false;
    }

    // Read from SLRU storage
    int slotno = SimpleLruReadPage_ReadOnly(CommitTsCtl, pageno, xid);
    memcpy(&entry, CommitTsCtl->shared->page_buffer[slotno] +
           SizeOfCommitTimestampEntry * entryno, SizeOfCommitTimestampEntry);

    *ts = entry.time;
    if (nodeid) *nodeid = entry.nodeid;

    LWLockRelease(SimpleLruGetBankLock(CommitTsCtl, pageno));
    return *ts != 0;
}
```