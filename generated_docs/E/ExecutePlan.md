# ExecutePlan

## Location
[src/backend/executor/execMain.c:1597-1718](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L1597-L1718)

## Overview
ExecutePlan is a static function that processes the query plan until a specified number of tuples have been retrieved, moving in the specified direction and handling parallel execution as needed.

## Definition
```c
static void ExecutePlan(QueryDesc *queryDesc, CmdType operation, bool sendTuples, 
                       uint64 numberTuples, ScanDirection direction, DestReceiver *dest)
```

## Detailed Description
ExecutePlan is the core execution engine that drives query plan processing in PostgreSQL. It runs the main execution loop, retrieving tuples from the plan tree and sending them to the destination. The function supports both complete execution (when numberTuples is 0) and limited execution (when a specific tuple count is requested).

Key execution features:
1. **Parallel Execution**: Manages parallel mode setup and teardown, but only for complete plan execution (partial execution forces sequential mode)
2. **Tuple Processing**: Implements the main execution loop using ExecProcNode to retrieve tuples
3. **Junk Filtering**: Removes junk attributes from tuples when a junk filter is present
4. **Direction Control**: Supports forward and backward scanning through the ScanDirection parameter
5. **Resource Management**: Handles proper cleanup including parallel mode exit and node shutdown when backward scanning isn't needed

The function maintains execution state and coordinates between the plan tree execution, tuple filtering, destination sending, and resource management.

## Parameters / Member Variables
- `queryDesc`: Pointer to QueryDesc containing the query execution context, including estate, planstate, and execution flags
- `operation`: CmdType indicating the type of SQL operation (SELECT, INSERT, UPDATE, DELETE) for proper tuple counting
- `sendTuples`: Boolean flag indicating whether tuples should be sent to the destination receiver
- `numberTuples`: Maximum number of tuples to process; 0 means process all tuples (run to completion)
- `direction`: ScanDirection specifying the scan direction (forward or backward)
- `dest`: Pointer to DestReceiver for sending processed tuples to their destination

## Dependencies
- Functions called/Symbols referenced:
  - [EnterParallelMode](EnterParallelMode.md)/ExitParallelMode (parallel execution control)
  - ResetPerTupleExprContext (expression context cleanup per tuple)
  - [ExecProcNode](ExecProcNode.md) (retrieves next tuple from plan tree)
  - TupIsNull (checks for null tuple indicating end of data)
  - [ExecFilterJunk](ExecFilterJunk.md) (removes junk attributes from tuples)
  - [ExecShutdownNode](ExecShutdownNode.md) (releases resources when backward scanning not needed)
  - Various constants: CMD_SELECT, EXEC_FLAG_BACKWARD
- Called from:
  - [standard_ExecutorRun](../s/standard_ExecutorRun.md) (main entry point for standard executor execution)

## Notes and Other Information
- This is a static function accessible only within execMain.c
- Parallel mode is only supported for complete execution; partial execution or already-executed queries force sequential mode
- The function sets `queryDesc->already_executed = true` to prevent subsequent parallel execution
- Tuple counting is handled differently for SELECT operations versus modification operations (INSERT/UPDATE/DELETE)
- Resource optimization: calls ExecShutdownNode early when backward scanning capability is not needed
- The main execution loop continues until: no more tuples, tuple limit reached, or destination stops accepting tuples
- Critical for query performance as it controls the fundamental tuple processing pipeline
- Coordinates closely with the destination receiver to handle cases where the destination closes early