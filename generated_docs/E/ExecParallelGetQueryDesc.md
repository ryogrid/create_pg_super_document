# ExecParallelGetQueryDesc

## Location
[src/backend/executor/execParallel.c:1236-1267](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execParallel.c#L1236-L1267)

## Overview
Creates a QueryDesc structure for a parallel worker by reconstructing the PlannedStmt and associated query information from shared memory.

## Definition
```c
static QueryDesc *ExecParallelGetQueryDesc(shm_toc *toc, DestReceiver *receiver, int instrument_options)
```

## Detailed Description
This static function is a key component of PostgreSQL's parallel query execution system. It reconstructs the complete query execution context for a parallel worker process by deserializing data structures that were previously stored in shared memory by the leader process. The function retrieves the query string, planned statement (PlannedStmt), and parameter list information from shared memory, then creates a QueryDesc that contains everything needed for the worker to execute its portion of the parallel query.

The function handles the deserialization of complex query structures using stringToNode() for the PlannedStmt and RestoreParamList() for parameters. It sets up the QueryDesc with the active snapshot and the provided DestReceiver for tuple output.

## Parameters / Member Variables
- `toc`: Shared memory table of contents used to locate query components in shared memory
- `receiver`: DestReceiver for tuple output (typically obtained from ExecParallelGetReceiver)
- `instrument_options`: Instrumentation flags controlling what execution statistics to collect

## Dependencies
- Functions called/Symbols referenced:
  - [shm_toc_lookup](../s/shm_toc_lookup.md)
  - [stringToNode](../s/stringToNode.md)
  - [RestoreParamList](../R/RestoreParamList.md)
  - [CreateQueryDesc](../C/CreateQueryDesc.md)
  - GetActiveSnapshot
- Constants used:
  - PARALLEL_KEY_QUERY_TEXT
  - PARALLEL_KEY_PLANNEDSTMT
  - PARALLEL_KEY_PARAMLISTINFO
  - InvalidSnapshot
- Types used:
  - [PlannedStmt](../P/PlannedStmt.md)
  - [ParamListInfo](../P/ParamListInfo.md)
  - QueryDesc
  - DestReceiver
- Called from:
  - [ParallelQueryMain](../P/ParallelQueryMain.md)

## Notes and Other Information
- This is a static function, only accessible within execParallel.c
- The function assumes all required data structures have been properly serialized to shared memory by the leader
- Uses the active snapshot to ensure consistent data visibility across parallel workers
- The returned QueryDesc contains everything needed for a worker to execute its portion of the parallel plan
- Parameter list restoration handles both simple and complex parameter types that may be passed to the query
- Part of the parallel query infrastructure that enables distributing query execution across multiple processes