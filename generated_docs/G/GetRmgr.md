# GetRmgr

## Location
[src/include/access/xlog_internal.h:376-405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlog_internal.h#L376-L405)

## Overview
GetRmgr is a static inline function that safely retrieves a resource manager entry from the RmgrTable, with validation to ensure the requested resource manager exists.

## Definition

```c
static inline RmgrData
GetRmgr(RmgrId rmid)
```
## Detailed Description
This function provides a safe way to access resource manager data from the RmgrTable by first validating that the specified resource manager ID exists using RmgrIdExists. If the resource manager ID is invalid, it calls RmgrNotFound to handle the error condition (typically by throwing an error). If valid, it returns the RmgrData structure from the table. This approach ensures that callers receive valid resource manager data and prevents accessing uninitialized or invalid table entries.

## Parameters / Member Variables
- `rmid`: RmgrId value representing the resource manager ID to retrieve

## Dependencies
- Functions called/Symbols referenced:
  - [RmgrIdExists](../R/RmgrIdExists.md) (for validation check)
  - [RmgrNotFound](../R/RmgrNotFound.md) (error handling for invalid IDs)
  - RmgrTable (global resource manager table accessed via array indexing)
- Additional references found in context:
  - [GetLastSegSwitchData](GetLastSegSwitchData.md), RequestXLogSwitch, GetOldestRestartPoint, XLogRecGetBlockRefInfo
- Called from (representative examples):
  - PG_GET_RESOURCE_MANAGERS_COLS (in src/backend/access/transam/rmgr.c)
  - [check_wal_consistency_checking](../c/check_wal_consistency_checking.md) (in src/backend/access/transam/xlog.c)
  - [ApplyWalRecord](../A/ApplyWalRecord.md) (in src/backend/access/transam/xlogrecovery.c)
  - [xlog_outdesc](../x/xlog_outdesc.md) (in src/backend/access/transam/xlogrecovery.c)
  - [LogicalDecodingProcessRecord](../L/LogicalDecodingProcessRecord.md) (in src/backend/replication/logical/decode.c)

## Notes and Other Information
The function uses the `unlikely` macro around the validation check, indicating that invalid resource manager IDs are expected to be rare in normal operation. This optimization hint helps the compiler generate more efficient code by predicting that the validation will usually succeed. The function is widely used throughout WAL processing, recovery, and logical decoding operations where resource manager information is needed.

## Simplified Source

```c
static inline RmgrData
GetRmgr(RmgrId rmid)
{
    // Check if resource manager ID is valid (rare failure case)
    if (unlikely(!RmgrIdExists(rmid)))
        RmgrNotFound(rmid);

    return RmgrTable[rmid];
}
```