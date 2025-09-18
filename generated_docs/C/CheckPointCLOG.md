# CheckPointCLOG

## Location
[src/backend/access/transam/clog.c:937-958](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/clog.c#L937-L958)

## Overview
CheckPointCLOG writes all dirty CLOG pages to disk as part of PostgreSQL's checkpoint process, ensuring transaction status data is safely persisted.

## Definition
```c
void CheckPointCLOG(void)
```

## Detailed Description
CheckPointCLOG is responsible for flushing all dirty CLOG (Commit Log) pages to disk during both shutdown checkpoints and on-the-fly checkpoints. The function ensures that all pending transaction status changes stored in memory are safely written to persistent storage. The write operations may queue sync requests that will be processed later by ProcessSyncRequests() as part of the overall checkpoint process.

The function includes tracing hooks for performance monitoring and debugging, marking the start and completion of the CLOG checkpoint operation.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - TRACE_POSTGRESQL_CLOG_CHECKPOINT_START
  - [SimpleLruWriteAll](../S/SimpleLruWriteAll.md)
  - TRACE_POSTGRESQL_CLOG_CHECKPOINT_DONE
- Global variables accessed:
  - XactCtl
- Called from:
  - [CheckPointGuts](CheckPointGuts.md) (src/backend/access/transam/xlog.c:7515)

## Notes and Other Information
- Can be called during both shutdown checkpoints and on-the-fly checkpoints
- Uses SimpleLruWriteAll with flush=true parameter to ensure immediate disk writes
- May result in sync requests being queued for later processing by ProcessSyncRequests()
- Includes PostgreSQL tracing hooks for performance monitoring and debugging
- Essential for ensuring ACID properties by guaranteeing transaction status persistence