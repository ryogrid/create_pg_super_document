# ExecInitSubPlan

## Location
[src/backend/executor/nodeSubplan.c:823-1091](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSubplan.c#L823-L1091)

## Overview
Initializes a SubPlanState structure for executing subplans and initplans, setting up all necessary data structures including hash tables, projection nodes, and equality functions.

## Definition
```c
SubPlanState *ExecInitSubPlan(SubPlan *subplan, PlanState *parent)
```

## Detailed Description
ExecInitSubPlan is the primary initialization function for SubPlan execution, responsible for creating and configuring a SubPlanState structure that contains all the runtime state needed to execute a subplan. This function is the SubPlan-specific part of ExecInitExpr() and handles both regular SubPlans and InitPlans.

The function performs several critical initialization tasks:
1. Links the SubPlanState to an already-initialized subplan from the estate's subplanstates list
2. Initializes test expressions and argument lists for subplan evaluation
3. Sets up output parameters for InitPlans that provide values to parent plans
4. For hashable subplans, creates memory contexts, projection nodes, and hash table infrastructure
5. Builds equality and hash function arrays for cross-type comparisons
6. Creates tuple descriptors and slots for left and right side evaluations

The function includes extensive logic for hash table setup when subplan.useHashTable is true, involving the extraction of combining operators, creation of target lists for both sides of comparisons, and initialization of all the hash and equality functions needed for efficient subplan execution.

## Parameters / Member Variables
- `subplan`: The SubPlan node containing the subplan definition and metadata
- `parent`: The parent PlanState that contains this subplan

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create SubPlanState)
  - [list_nth](../l/list_nth.md) (to access subplan from estate)
  - [ExecInitExpr](ExecInitExpr.md), ExecInitExprList (for expression initialization)
  - AllocSetContextCreate (for memory context creation)
  - [CreateExprContext](../C/CreateExprContext.md) (for expression evaluation context)
  - [ExecTypeFromTL](ExecTypeFromTL.md), ExecInitExtraTupleSlot (for tuple handling)
  - [ExecBuildProjectionInfo](ExecBuildProjectionInfo.md), ExecBuildGroupingEqual (for projection setup)
  - [get_compatible_hash_operators](../g/get_compatible_hash_operators.md), get_op_hash_functions (for hash function setup)
  - [fmgr_info](../f/fmgr_info.md), fmgr_info_set_expr (for function manager setup)
- Types used:
  - [SubPlan](../S/SubPlan.md), SubPlanState, PlanState, EState
  - [ParamExecData](../P/ParamExecData.md), OpExpr, BoolExpr, TargetEntry
  - [TupleDesc](../T/TupleDesc.md), TupleTableSlot
- Called from (representative examples):
  - [ExecInitExprRec](ExecInitExprRec.md) (during expression tree initialization)
  - [ExecPushExprSetupSteps](ExecPushExprSetupSteps.md) (for compiled expression setup)
  - [ExecInitNode](ExecInitNode.md) (during plan node initialization)

## Notes and Other Information
- This function does not link the SubPlan into the parent's subPlan list - that's handled by ExecInitExpr()
- The function includes error checking for parallel-unsafe subplans in parallelized subqueries
- For InitPlans, sets up parameter execution data to mark output parameters as needing evaluation
- [Hash](../H/Hash.md) table creation is deferred until actually needed, but all supporting infrastructure is initialized
- The function uses a 'hack alert' approach where lefthand expressions use NULL exprcontext initially, filled in later
- Memory contexts are carefully structured: hashtablecxt for main storage, hashtempcxt for temporary operations
- Column indexing uses 1-based numbering (keyColIdx) consistent with PostgreSQL conventions
- Cross-type equality functions are set up separately from same-type hash table functions to support type coercion scenarios

## Simplified Source

```c
SubPlanState *
ExecInitSubPlan(SubPlan *subplan, PlanState *parent)
{
    SubPlanState *sstate = makeNode(SubPlanState);
    EState *estate = parent->state;

    sstate->subplan = subplan;

    // Link to already-initialized subplan
    sstate->planstate = (PlanState *) list_nth(estate->es_subplanstates,
                                              subplan->plan_id - 1);

    if (sstate->planstate == NULL)
        elog(ERROR, "subplan \"%s\" was not initialized", subplan->plan_name);

    sstate->parent = parent;

    // Initialize subexpressions
    sstate->testexpr = ExecInitExpr((Expr *) subplan->testexpr, parent);
    sstate->args = ExecInitExprList(subplan->args, parent);

    // Initialize state variables to NULL/default values
    initialize_subplan_state_variables(sstate);

    // Handle InitPlan output parameters
    if (subplan->setParam != NIL && subplan->parParam == NIL &&
        subplan->subLinkType != CTE_SUBLINK)
    {
        foreach(lst, subplan->setParam)
        {
            int paramid = lfirst_int(lst);
            ParamExecData *prm = &(estate->es_param_exec_vals[paramid]);
            prm->execPlan = sstate;
        }
    }

    // Initialize hash table infrastructure if needed
    if (subplan->useHashTable)
    {
        // Create memory contexts for hash operations
        sstate->hashtablecxt = AllocSetContextCreate(CurrentMemoryContext,
                                                    "Subplan HashTable Context",
                                                    ALLOCSET_DEFAULT_SIZES);
        sstate->hashtempcxt = AllocSetContextCreate(CurrentMemoryContext,
                                                   "Subplan HashTable Temp Context",
                                                   ALLOCSET_SMALL_SIZES);
        sstate->innerecontext = CreateExprContext(estate);

        // Extract operator list from test expression
        List *oplist = extract_operator_list(subplan->testexpr);
        int ncols = list_length(oplist);

        // Allocate arrays for hash/equality functions
        allocate_function_arrays(sstate, ncols);

        // Process each operator to build target lists and function info
        List *lefttlist = NIL, *righttlist = NIL;
        int i = 1;
        foreach(l, oplist)
        {
            OpExpr *opexpr = lfirst_node(OpExpr, l);

            // Build target entries for left and right sides
            build_target_entries(opexpr, &lefttlist, &righttlist, i);

            // Set up equality and hash functions
            setup_hash_and_equality_functions(sstate, opexpr, i - 1);

            i++;
        }

        // Create projection nodes and tuple descriptors
        setup_projection_nodes(sstate, estate, parent, lefttlist, righttlist);

        // Create cross-type comparator
        sstate->cur_eq_comp = ExecBuildGroupingEqual(tupDescLeft, tupDescRight,
                                                    &TTSOpsVirtual, &TTSOpsMinimalTuple,
                                                    ncols, sstate->keyColIdx,
                                                    cross_eq_funcoids,
                                                    sstate->tab_collations, parent);
    }

    return sstate;
}
```