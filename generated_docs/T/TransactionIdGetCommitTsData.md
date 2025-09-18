# TransactionIdGetCommitTsData

## Location
src/backend/access/transam/commit_ts.c: 274 - 359

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
  - TransactionIdToCTsPage (to determine the SLRU page)
  - TransactionIdToCTsEntry (to calculate entry offset within page)
  - TransactionIdIsValid/TransactionIdIsNormal (validation functions)
  - TransactionIdPrecedes (for range checking)
  - error_commit_ts_disabled (error reporting when feature disabled)
  - SimpleLruReadPage_ReadOnly (to read SLRU page)
  - SimpleLruGetBankLock (to get page lock for release)
  - CommitTimestampEntry (structure for timestamp data)
- Called from (representative examples):
  - pg_xact_commit_timestamp (SQL function interface)
  - pg_xact_commit_timestamp_origin (SQL function interface)

## Notes and Other Information
- Uses shared lock (LW_SHARED) for reading commit timestamp shared state
- Implements caching optimization for recently committed transactions
- Handles bootstrap and frozen XIDs as special cases (always return false)
- Validates that requested XID is within the valid commit timestamp range
- Uses read-only SLRU access when reading from disk storage
- Returns false (with timestamp 0) when no valid timestamp data exists
- Critical function for commit timestamp queries and replication scenarios
- Location: src/backend/access/transam/commit_ts.c:274-359