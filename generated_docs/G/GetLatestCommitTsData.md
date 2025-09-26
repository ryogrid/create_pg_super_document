# GetLatestCommitTsData

## Location
[src/backend/access/transam/commit_ts.c:360-380](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/commit_ts.c#L360-L380)

## Overview
Returns the transaction ID of the latest committed transaction along with its commit timestamp and replication origin node ID.

## Definition

```c
TransactionId
GetLatestCommitTsData(TimestampTz *ts, RepOriginId *nodeid)
```
## Detailed Description
This function retrieves information about the most recently committed transaction from the commit timestamp module. It acquires a shared lock on the commit timestamp data structure to safely read the latest commit information. The function is designed to provide transactional metadata that can be useful for replication, monitoring, and debugging purposes. The caller must ensure that the commit timestamp tracking feature is enabled before calling this function.

## Parameters / Member Variables
- : Optional output parameter (can be NULL) that receives the commit timestamp of the latest committed transaction
- : Optional output parameter (can be NULL) that receives the replication origin node ID associated with the latest committed transaction

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md) (CommitTsLock, LW_SHARED)
  - [error_commit_ts_disabled](../e/error_commit_ts_disabled.md)
  - [LWLockRelease](../L/LWLockRelease.md) (CommitTsLock)
- Called from (representative examples):
  - [pg_last_committed_xact](../p/pg_last_committed_xact.md)

## Notes and Other Information
- The function throws an error if the commit timestamp module is not enabled (commitTsShared->commitTsActive is false)
- Uses shared locking to allow concurrent reads while preventing inconsistent data during updates
- Both output parameters are optional and can be passed as NULL if the corresponding data is not needed
- Located in src/backend/access/transam/commit_ts.c:360-380
- Part of PostgreSQL's commit timestamp tracking infrastructure used primarily for logical replication and debugging