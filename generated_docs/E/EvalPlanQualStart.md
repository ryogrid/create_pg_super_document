# EvalPlanQualStart

## Location
[src/backend/executor/execMain.c:2822-2985](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L2822-L2985)

## Overview
EvalPlanQualStart initializes and starts execution of an EvalPlanQual plan tree by creating a separate EState that shares resources with the parent query.

## Definition
```c
static void EvalPlanQualStart(EPQState *epqstate, Plan *planTree)
```

## Detailed Description
EvalPlanQualStart is a cut-down version of ExecutorStart() that prepares an EPQ (EvalPlanQual) execution context for rechecking candidate tuples. The function creates a new EState (recheckestate) that shares unchanging state like snapshots and range tables from the parent EState while maintaining its own local state including tuple tables, parameter execution values, and result relation information.

The function performs several key operations:
1. Creates a new executor state using CreateExecutorState()
2. Copies shared state from the parent EState (snapshots, range tables, external parameters)
3. Initializes local state including parameter workspaces and subplan states
4. Sets up rowmark arrays for efficient tuple fetching
5. Initializes per-relation EPQ tuple tracking arrays
6. Initializes the plan tree nodes for execution

This setup allows EPQ to re-execute portions of a query plan with specific tuple substitutions to handle concurrent modifications in READ COMMITTED isolation level.

## Parameters / Member Variables
- `epqstate`: Pointer to EPQState structure containing EPQ execution context
- `planTree`: The plan tree that needs to be executed for the EPQ recheck

## Dependencies
- Functions called/Symbols referenced:
  - [CreateExecutorState](../C/CreateExecutorState.md)
  - [ExecSetParamPlanMulti](ExecSetParamPlanMulti.md)  
  - GetPerTupleExprContext
  - [ExecInitNode](ExecInitNode.md)
  - palloc_array
  - palloc0_array
  - lfirst_int
  - [ParamExecData](../P/ParamExecData.md)
  - [ExecAuxRowMark](ExecAuxRowMark.md)
  - ForwardScanDirection

- Called from (representative examples):
  - [EvalPlanQualBegin](EvalPlanQualBegin.md)

## Notes and Other Information
- This function is static and only used internally within execMain.c
- The created EState shares most state with the parent but maintains separate copies of local state like tuple tables and parameter execution values
- [Result](../R/Result.md) relations in the EPQ context are marked as blocked initially
- All subplans from the parent planned statement are initialized even if not all will be used
- The function operates within the es_query_cxt memory context of the newly created EState
- EPQ is primarily used in ModifyTable and LockRows operations to handle concurrent tuple modifications