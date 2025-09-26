# postquel_start

## Location
[src/backend/executor/functions.c:814-875](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/functions.c#L814-L875)

## Overview
Starts execution of a single execution state node by creating query descriptors and initializing the executor for a SQL function statement.

## Definition
```c
static void
postquel_start(execution_state *es, SQLFunctionCachePtr fcache)
```

## Detailed Description
This function initiates execution of one query within a SQL function by setting up the necessary execution infrastructure. It determines the appropriate destination receiver based on whether the query produces function results, creates a query descriptor with the planned statement, and starts the executor for non-utility commands. The function handles both result-producing queries (which send output to the tuplestore) and non-result queries (which discard output). For lazy evaluation mode, it configures executor flags to skip trigger processing.

## Parameters / Member Variables
- `es`: Pointer to execution_state structure representing the query to be started
- `fcache`: Pointer to SQL function cache containing function execution context and metadata

## Dependencies
- Functions called/Symbols referenced:
  - [ActiveSnapshotSet](../A/ActiveSnapshotSet.md)
  - [CreateDestReceiver](../C/CreateDestReceiver.md)
  - [CreateQueryDesc](../C/CreateQueryDesc.md)
  - [GetActiveSnapshot](../G/GetActiveSnapshot.md)
  - [ExecutorStart](../E/ExecutorStart.md)
  - None_Receiver (destination receiver)
  - DR_sqlfunction (destination receiver type)
- Called from (representative examples):
  - [fmgr_sql](../f/fmgr_sql.md)

## Notes and Other Information
- Asserts that no query descriptor exists yet (es->qd == NULL) and active snapshot is set
- Configures DestSQLFunction receiver for result-producing queries with tuplestore, memory context, and junk filter
- Uses None_Receiver to discard output from non-result-producing queries
- Sets EXEC_FLAG_SKIP_TRIGGERS for lazy evaluation to prevent AfterTrigger context stacking issues
- Utility commands bypass executor initialization as they don't require planning infrastructure
- Updates execution state status to F_EXEC_RUN after successful startup