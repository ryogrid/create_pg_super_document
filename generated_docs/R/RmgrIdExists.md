# RmgrIdExists

## Location
[src/include/access/xlog_internal.h:370-375](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlog_internal.h#L370-L375)

## Overview
RmgrIdExists is a static inline function that checks whether a given resource manager ID corresponds to a valid, registered resource manager in PostgreSQL's WAL system.

## Definition

```c
static inline bool
RmgrIdExists(RmgrId rmid)
```
## Detailed Description
This function validates the existence of a resource manager by checking if the specified RmgrId has a valid entry in the RmgrTable. It performs this check by examining whether the rm_name field for the given resource manager ID is non-NULL, which indicates that a resource manager has been properly registered at that ID. This validation is essential for ensuring that WAL record processing and resource manager operations are performed only on valid, registered resource managers.

## Parameters / Member Variables
- `rmid`: RmgrId value representing the resource manager ID to validate

## Dependencies
- Functions called/Symbols referenced:
  - RmgrTable (global resource manager table accessed via array indexing)
  - RmgrData (struct type containing rm_name field)
- Called from (representative examples):
  - RmgrStartup (in src/backend/access/transam/rmgr.c)
  - RmgrCleanup (in src/backend/access/transam/rmgr.c)
  - RegisterCustomRmgr (in src/backend/access/transam/rmgr.c)
  - GetRmgr (in src/include/access/xlog_internal.h)
  - check_wal_consistency_checking (in src/backend/access/transam/xlog.c)

## Notes and Other Information
The function provides a lightweight way to validate resource manager IDs before performing operations that assume the resource manager exists. It's commonly used as a guard condition in resource manager initialization, cleanup, and consistency checking operations. The validation relies on the convention that registered resource managers have non-NULL rm_name fields in the RmgrTable.