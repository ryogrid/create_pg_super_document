# error_commit_ts_disabled

## Location
[src/backend/access/transam/commit_ts.c:381-396](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/commit_ts.c#L381-L396)

## Overview
A static error reporting function that generates a standardized error message when commit timestamp functionality is accessed while disabled.

## Definition
```c
static void error_commit_ts_disabled(void)
```

## Detailed Description
This internal helper function generates a consistent error message when code attempts to access commit timestamp data but the track_commit_timestamp configuration parameter is not enabled. The function provides different error hints depending on whether the server is in recovery mode or not, offering appropriate guidance to users on how to resolve the issue.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - ereport (ERROR)
  - [errcode](errcode.md) (ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
  - [errmsg](errmsg.md)
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - [errhint](errhint.md)
- Called from (representative examples):
  - [TransactionIdGetCommitTsData](../T/TransactionIdGetCommitTsData.md)
  - [GetLatestCommitTsData](../G/GetLatestCommitTsData.md)

## Notes and Other Information
- Static function with internal linkage, only accessible within commit_ts.c
- Uses different error hints for primary servers vs servers in recovery mode
- Error code ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE indicates the feature is not properly configured
- Located in src/backend/access/transam/commit_ts.c:381-396
- Part of PostgreSQL's error handling infrastructure for commit timestamp tracking
- The error message guides users to enable the track_commit_timestamp configuration parameter