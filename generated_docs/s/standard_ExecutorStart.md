# standard_ExecutorStart

## Location
[src/backend/executor/execMain.c:140-298](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L140-L298)

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
  - [GetActiveSnapshot](../G/GetActiveSnapshot.md) (validates active snapshot)
  - [IsInParallelMode](../I/IsInParallelMode.md) (checks parallel execution mode)
  - [ExecCheckXactReadOnly](../E/ExecCheckXactReadOnly.md) (validates read-only transaction compliance)
  - [GetCurrentCommandId](../G/GetCurrentCommandId.md) (obtains command ID for tuple marking)
  - [RegisterSnapshot](../R/RegisterSnapshot.md) (registers snapshots for MVCC)
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

## Simplified Source

```c
// Simplified version of standard_ExecutorStart
void standard_ExecutorStart(QueryDesc *queryDesc, int eflags) {
    EState *estate;
    MemoryContext oldcontext;

    // Basic sanity checks
    Assert(queryDesc != NULL);
    Assert(queryDesc->estate == NULL);
    Assert(GetActiveSnapshot() == queryDesc->snapshot);

    // Check for read-only transaction violations and parallel mode safety
    if ((XactReadOnly || IsInParallelMode()) &&
        !(eflags & EXEC_FLAG_EXPLAIN_ONLY)) {
        ExecCheckXactReadOnly(queryDesc->plannedstmt);
    }

    // Create execution state and switch to query memory context
    estate = CreateExecutorState();
    queryDesc->estate = estate;
    oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

    // Set up parameter information
    estate->es_param_list_info = queryDesc->params;

    // Allocate space for internal parameters if needed
    if (queryDesc->plannedstmt->paramExecTypes != NIL) {
        int nParamExec = list_length(queryDesc->plannedstmt->paramExecTypes);
        estate->es_param_exec_vals =
            palloc0(nParamExec * sizeof(ParamExecData));
    }

    // Copy essential query information
    estate->es_sourceText = queryDesc->sourceText;
    estate->es_queryEnv = queryDesc->queryEnv;

    // Set command ID based on operation type for tuple marking
    switch (queryDesc->operation) {
        case CMD_SELECT:
            // Mark tuples for SELECT FOR UPDATE/SHARE or modifying CTEs
            if (queryDesc->plannedstmt->rowMarks != NIL ||
                queryDesc->plannedstmt->hasModifyingCTE) {
                estate->es_output_cid = GetCurrentCommandId(true);
            }
            // Skip triggers for simple SELECT queries
            if (!queryDesc->plannedstmt->hasModifyingCTE) {
                eflags |= EXEC_FLAG_SKIP_TRIGGERS;
            }
            break;

        case CMD_INSERT:
        case CMD_DELETE:
        case CMD_UPDATE:
        case CMD_MERGE:
            estate->es_output_cid = GetCurrentCommandId(true);
            break;

        default:
            elog(ERROR, "unrecognized operation code: %d",
                 (int) queryDesc->operation);
    }

    // Register snapshots and copy execution settings
    estate->es_snapshot = RegisterSnapshot(queryDesc->snapshot);
    estate->es_crosscheck_snapshot =
        RegisterSnapshot(queryDesc->crosscheck_snapshot);
    estate->es_top_eflags = eflags;
    estate->es_instrument = queryDesc->instrument_options;
    estate->es_jit_flags = queryDesc->plannedstmt->jitFlags;

    // Set up trigger context if needed
    if (!(eflags & (EXEC_FLAG_SKIP_TRIGGERS | EXEC_FLAG_EXPLAIN_ONLY))) {
        AfterTriggerBeginQuery();
    }

    // Initialize the plan state tree
    InitPlan(queryDesc, eflags);

    // Restore previous memory context
    MemoryContextSwitchTo(oldcontext);
}
```

Key simplifications made:
- Consolidated comment blocks into brief single-line descriptions
- Removed detailed multi-line comments while preserving essential logic explanations
- Simplified variable declarations and memory allocation patterns
- Maintained all critical assertions and error checks
- Preserved the complete switch statement logic for different command types
- Kept essential memory context management and cleanup
- Retained all function calls that perform core initialization work