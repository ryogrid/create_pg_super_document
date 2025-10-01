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

## Simplified Source

```c
static void EvalPlanQualStart(EPQState *epqstate, Plan *planTree) {
    EState *parentestate = epqstate->parentestate;
    Index rtsize = parentestate->es_range_table_size;

    // Create new executor state for EPQ recheck
    EState *rcestate = CreateExecutorState();
    epqstate->recheckestate = rcestate;

    MemoryContext oldcontext = MemoryContextSwitchTo(rcestate->es_query_cxt);

    // Mark this as an EPQ EState
    rcestate->es_epq_active = epqstate;

    // Copy shared unchanging state from parent
    rcestate->es_direction = ForwardScanDirection;
    rcestate->es_snapshot = parentestate->es_snapshot;
    rcestate->es_crosscheck_snapshot = parentestate->es_crosscheck_snapshot;
    rcestate->es_range_table = parentestate->es_range_table;
    rcestate->es_range_table_size = parentestate->es_range_table_size;
    rcestate->es_relations = parentestate->es_relations;
    rcestate->es_rowmarks = parentestate->es_rowmarks;
    rcestate->es_plannedstmt = parentestate->es_plannedstmt;
    rcestate->es_param_list_info = parentestate->es_param_list_info;

    // Initialize local EPQ state
    rcestate->es_result_relations = NULL;

    // Set up parameter execution values if needed
    if (parentestate->es_plannedstmt->paramExecTypes != NIL) {
        // Force evaluation of InitPlan outputs
        ExecSetParamPlanMulti(planTree->extParam,
                             GetPerTupleExprContext(parentestate));

        // Create local parameter workspace and copy values
        int param_count = list_length(parentestate->es_plannedstmt->paramExecTypes);
        rcestate->es_param_exec_vals = palloc0(param_count * sizeof(ParamExecData));

        for (int i = 0; i < param_count; i++) {
            rcestate->es_param_exec_vals[i].value =
                parentestate->es_param_exec_vals[i].value;
            rcestate->es_param_exec_vals[i].isnull =
                parentestate->es_param_exec_vals[i].isnull;
        }
    }

    // Initialize all subplans from parent
    foreach(ListCell *l, parentestate->es_plannedstmt->subplans) {
        Plan *subplan = (Plan *) lfirst(l);
        PlanState *subplanstate = ExecInitNode(subplan, rcestate, 0);
        rcestate->es_subplanstates = lappend(rcestate->es_subplanstates, subplanstate);
    }

    // Build RTI-indexed array of rowmarks for efficient access
    epqstate->relsubs_rowmark = palloc0(rtsize * sizeof(ExecAuxRowMark *));
    foreach(ListCell *l, epqstate->arowMarks) {
        ExecAuxRowMark *earm = (ExecAuxRowMark *) lfirst(l);
        epqstate->relsubs_rowmark[earm->rowmark->rti - 1] = earm;
    }

    // Initialize per-relation EPQ tuple states
    epqstate->relsubs_done = palloc_array(bool, rtsize);
    epqstate->relsubs_blocked = palloc0_array(bool, rtsize);

    // Mark result relations as blocked
    foreach(ListCell *l, epqstate->resultRelations) {
        int rtindex = lfirst_int(l);
        epqstate->relsubs_blocked[rtindex - 1] = true;
    }
    memcpy(epqstate->relsubs_done, epqstate->relsubs_blocked, rtsize * sizeof(bool));

    // Initialize the plan tree for execution
    epqstate->recheckplanstate = ExecInitNode(planTree, rcestate, 0);

    MemoryContextSwitchTo(oldcontext);
}
```