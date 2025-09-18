# standard_ExecutorStart

## Location
src/backend/executor/execMain.c: 140 - 298

## Overview
standard_ExecutorStart is the default implementation that performs the actual executor initialization work, including creating the execution state, setting up parameters, and initializing the plan state tree.

## Definition
```c
void standard_ExecutorStart(QueryDesc *queryDesc, int eflags)
```

## Detailed Description
standard_ExecutorStart performs the comprehensive initialization required to begin query execution. It creates an EState (execution state) structure that serves as the root of working storage for the entire executor invocation, switches to a per-query memory context, and sets up all necessary execution parameters and state.

The function handles several critical aspects:
- Transaction and parallel mode safety checks to prevent writes in read-only transactions or unsafe operations in parallel mode
- Parameter handling for both external parameters from the query descriptor and internal executor parameters (ParamExecData)
- Command ID assignment for tuple marking based on the operation type (SELECT, INSERT, UPDATE, DELETE, MERGE)
- Snapshot registration for MVCC consistency
- Trigger context setup when appropriate
- Plan state tree initialization through InitPlan

The function also implements important optimizations, such as enabling skip-triggers mode for SELECT queries without modifying CTEs.

## Parameters / Member Variables
- `queryDesc`: A QueryDesc structure containing the parsed and planned query information, must not be started already and must have an active snapshot
- `eflags`: Execution flags that control various aspects of execution behavior, including EXEC_FLAG_EXPLAIN_ONLY and EXEC_FLAG_SKIP_TRIGGERS

## Dependencies
- Functions called/Symbols referenced:
  - [CreateExecutorState](../C/CreateExecutorState.md) (creates the main execution state structure)
  - GetActiveSnapshot (validates active snapshot)
  - IsInParallelMode (checks parallel execution mode)
  - [ExecCheckXactReadOnly](../E/ExecCheckXactReadOnly.md) (validates read-only transaction compliance)
  - [GetCurrentCommandId](../G/GetCurrentCommandId.md) (obtains command ID for tuple marking)
  - RegisterSnapshot (registers snapshots for MVCC)
  - [AfterTriggerBeginQuery](../A/AfterTriggerBeginQuery.md) (sets up trigger context)
  - [InitPlan](../I/InitPlan.md) (initializes the plan state tree)
  - [ParamExecData](../P/ParamExecData.md) (internal parameter execution data structure)
- Called from (representative examples):
  - [ExecutorStart](../E/ExecutorStart.md) (src/backend/executor/execMain.c:136)

## Notes and Other Information
- The function includes extensive safety checks including assertions for proper calling sequence and active snapshots
- Memory context management ensures proper cleanup and isolation of executor memory usage
- Support for various SQL command types (SELECT, INSERT, UPDATE, DELETE, MERGE) with appropriate command ID handling
- Optimized trigger handling that skips trigger setup for read-only SELECT operations
- Located at src/backend/executor/execMain.c:140-298
- Parallel mode restrictions prevent certain operations that could cause data inconsistency
- The function handles both simple queries and complex operations with modifying CTEs and row marks