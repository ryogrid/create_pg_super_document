# ProcessQuery

## Location
src/backend/tcop/pquery.c: 136 - 208

## Overview
ProcessQuery executes a single plannable query within a portal context, handling the complete lifecycle from query descriptor creation through execution to cleanup.

## Definition
```c
static void ProcessQuery(PlannedStmt *plan,
                        const char *sourceText,
                        ParamListInfo params,
                        QueryEnvironment *queryEnv,
                        DestReceiver *dest,
                        QueryCompletion *qc)
```

## Detailed Description
ProcessQuery orchestrates the complete execution of a single planned query statement. It creates a QueryDesc object using CreateQueryDesc, executes the query through the PostgreSQL executor framework (ExecutorStart, ExecutorRun, ExecutorFinish, ExecutorEnd), and handles command completion status reporting. The function is designed to work within portal contexts like PORTAL_MULTI_QUERY, PORTAL_ONE_RETURNING, or PORTAL_ONE_MOD_WITH. It ensures proper resource cleanup by freeing the QueryDesc object after execution. The function also builds appropriate completion status based on the command type (SELECT, INSERT, UPDATE, DELETE, MERGE).

## Parameters / Member Variables
- `plan`: The planned statement tree containing the execution plan
- `sourceText`: The original SQL query text for debugging and logging
- `params`: Parameter values to be substituted into the query
- `queryEnv`: Query environment containing additional execution context
- `dest`: Destination receiver that will handle query output
- `qc`: Optional query completion status object to store execution results

## Dependencies
- Functions called/Symbols referenced:
  - [CreateQueryDesc](../C/CreateQueryDesc.md)
  - [ExecutorStart](../E/ExecutorStart.md)
  - [ExecutorRun](../E/ExecutorRun.md)
  - [ExecutorFinish](../E/ExecutorFinish.md)
  - [ExecutorEnd](../E/ExecutorEnd.md)
  - [FreeQueryDesc](../F/FreeQueryDesc.md)
  - GetActiveSnapshot
  - SetQueryCompletion
  - ForwardScanDirection
  - InvalidSnapshot
- Called from (representative examples):
  - [PortalRunMulti](PortalRunMulti.md)

## Notes and Other Information
This is a static function intended for internal use within the pquery.c module. The function must be called within a memory context that will be reset or deleted on error to prevent memory leaks from the executor. The query completion parameter (qc) is optional and may be NULL if the caller doesn't need status information. The function uses the active snapshot for query execution and no crosscheck snapshot (InvalidSnapshot).