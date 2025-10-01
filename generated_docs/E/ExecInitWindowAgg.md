# ExecInitWindowAgg

## Location
[src/backend/executor/nodeWindowAgg.c:2374-2680](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L2374-L2680)

## Overview
Initialization function for WindowAgg execution nodes that creates the runtime state structure, sets up window function processing infrastructure, and prepares all necessary components for window aggregation execution.

## Definition

```c
structure
	 */
	winstate = makeNode(WindowAggState);
```
## Detailed Description
ExecInitWindowAgg is the comprehensive initialization function for window aggregation nodes in PostgreSQL's executor. It transforms the planner-generated WindowAgg plan node into a fully operational WindowAggState execution state, setting up all the complex infrastructure needed for window function processing.

The function performs several major initialization tasks:

**State Structure Setup**: Creates the WindowAggState node and links it to the execution engine, setting ExecWindowAgg as the execution function.

**Memory Context Management**: Establishes multiple specialized memory contexts:
- General expression contexts for per-input and per-output tuple processing
- Partition context for partition-local storage that persists across rows in a partition  
- Aggregate context for window aggregate transition values

**Tuple Slot Initialization**: Creates various tuple slots for different purposes:
- Scan slots for input/output processing
- Temporary slots for intermediate computations
- Conditional frame head/tail slots for RANGE/GROUPS mode boundary tracking

**Expression and Qualification Setup**: Initializes filter qualifications and run conditions that enable performance optimizations like pass-through mode.

**Window Function Processing Setup**: Analyzes all window functions to:
- Detect and deduplicate identical window functions
- Set up per-function state including result types and collations
- Distinguish between plain aggregates (winagg=true) and true window functions
- Initialize function call infrastructure and permissions checking

**Aggregate Infrastructure**: For window functions that are actually aggregates, sets up the traditional aggregate processing machinery including WindowObject structures.

**Frame Boundary Support**: Initializes offset expressions and in_range support functions needed for RANGE mode frame boundary calculations.

**Comparison Function Setup**: Prepares tuple comparison functions for PARTITION BY and ORDER BY clauses.

## Parameters / Member Variables
- : WindowAgg plan node containing planner specifications including:
- : Executor state providing global execution context
- : Execution flags (BACKWARD and MARK not supported for WindowAgg)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (state structure creation)
  - [ExecAssignExprContext](ExecAssignExprContext.md) (expression context setup)
  - AllocSetContextCreate (memory context creation)  
  - [ExecInitQual](ExecInitQual.md) (qualification expression initialization)
  - [ExecInitNode](ExecInitNode.md) (child node initialization)
  - [ExecCreateScanSlotFromOuterPlan](ExecCreateScanSlotFromOuterPlan.md)/ExecInitExtraTupleSlot (tuple slot setup)
  - [ExecInitResultTupleSlotTL](ExecInitResultTupleSlotTL.md)/ExecAssignProjectionInfo (projection setup)
  - [execTuplesMatchPrepare](../e/execTuplesMatchPrepare.md) (tuple comparison function preparation)
  - [object_aclcheck](../o/object_aclcheck.md)/aclcheck_error (permission checking)
  - [get_typlenbyval](../g/get_typlenbyval.md) (type information retrieval)
  - [initialize_peragg](../i/initialize_peragg.md) (aggregate state setup)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)/fmgr_info_set_expr (function call setup)
  - [ExecInitExpr](ExecInitExpr.md) (expression initialization)
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md) (general executor node initialization dispatch)

## Notes and Other Information
- Returns fully initialized WindowAggState ready for execution via ExecWindowAgg
- Handles deduplication of identical window functions for performance optimization
- Only top-level WindowAgg nodes may have filter qualifications
- Sets up pass-through mode capabilities when run conditions are present
- Performs comprehensive permission checking for all window functions
- Creates specialized WindowObject structures that provide the interface between window functions and the execution engine
- Frame head/tail slots are created conditionally based on frame options to minimize memory usage
- Uses query-lifetime memory contexts for long-lived data like offset values and function call information
- Critical initialization path that must correctly set up all state for complex multi-function window processing

## Simplified Source

```c
WindowAggState *
ExecInitWindowAgg(WindowAgg *node, EState *estate, int eflags)
{
    WindowAggState *winstate;
    Plan *outerPlan;
    ExprContext *econtext;
    WindowStatePerFunc perfunc;
    WindowStatePerAgg peragg;
    int frameOptions = node->frameOptions;
    int numfuncs, numaggs, wfuncno, aggno;
    TupleDesc scanDesc;
    ListCell *l;

    // Check for unsupported execution flags
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    // Create and initialize state structure
    winstate = makeNode(WindowAggState);
    winstate->ss.ps.plan = (Plan *) node;
    winstate->ss.ps.state = estate;
    winstate->ss.ps.ExecProcNode = ExecWindowAgg;
    winstate->frameOptions = frameOptions;

    // Create dual expression contexts for per-input and per-output processing
    ExecAssignExprContext(estate, &winstate->ss.ps);
    winstate->tmpcontext = winstate->ss.ps.ps_ExprContext;
    ExecAssignExprContext(estate, &winstate->ss.ps);

    // Create specialized memory contexts
    winstate->partcontext = AllocSetContextCreate(CurrentMemoryContext,
                                                 "WindowAgg Partition",
                                                 ALLOCSET_DEFAULT_SIZES);
    winstate->aggcontext = AllocSetContextCreate(CurrentMemoryContext,
                                                "WindowAgg Aggregates",
                                                ALLOCSET_DEFAULT_SIZES);

    // Initialize qualifications and run conditions
    Assert(node->plan.qual == NIL || node->topWindow);
    winstate->ss.ps.qual = ExecInitQual(node->plan.qual, (PlanState *) winstate);
    winstate->runcondition = ExecInitQual(node->runCondition, (PlanState *) winstate);

    // Set up pass-through mode configuration
    winstate->use_pass_through = !node->topWindow || node->partNumCols > 0;
    winstate->top_window = node->topWindow;

    // Initialize outer plan
    outerPlan = outerPlan(node);
    outerPlanState(winstate) = ExecInitNode(outerPlan, estate, eflags);

    // Initialize tuple slots and result projection
    ExecCreateScanSlotFromOuterPlan(estate, &winstate->ss, &TTSOpsMinimalTuple);
    scanDesc = winstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor;

    winstate->ss.ps.outeropsset = true;
    winstate->ss.ps.outerops = &TTSOpsMinimalTuple;
    winstate->ss.ps.outeropsfixed = true;

    // Initialize additional tuple slots
    winstate->first_part_slot = ExecInitExtraTupleSlot(estate, scanDesc, &TTSOpsMinimalTuple);
    winstate->agg_row_slot = ExecInitExtraTupleSlot(estate, scanDesc, &TTSOpsMinimalTuple);
    winstate->temp_slot_1 = ExecInitExtraTupleSlot(estate, scanDesc, &TTSOpsMinimalTuple);
    winstate->temp_slot_2 = ExecInitExtraTupleSlot(estate, scanDesc, &TTSOpsMinimalTuple);

    // Create frame head/tail slots if needed for RANGE/GROUPS mode
    winstate->framehead_slot = winstate->frametail_slot = NULL;
    if (frameOptions & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS)) {
        if (((frameOptions & FRAMEOPTION_START_CURRENT_ROW) && node->ordNumCols != 0) ||
            (frameOptions & FRAMEOPTION_START_OFFSET))
            winstate->framehead_slot = ExecInitExtraTupleSlot(estate, scanDesc, &TTSOpsMinimalTuple);
        if (((frameOptions & FRAMEOPTION_END_CURRENT_ROW) && node->ordNumCols != 0) ||
            (frameOptions & FRAMEOPTION_END_OFFSET))
            winstate->frametail_slot = ExecInitExtraTupleSlot(estate, scanDesc, &TTSOpsMinimalTuple);
    }

    // Initialize result projection
    ExecInitResultTupleSlotTL(&winstate->ss.ps, &TTSOpsVirtual);
    ExecAssignProjectionInfo(&winstate->ss.ps, NULL);

    // Set up tuple comparison functions for PARTITION BY and ORDER BY
    if (node->partNumCols > 0)
        winstate->partEqfunction = execTuplesMatchPrepare(scanDesc, node->partNumCols,
                                                         node->partColIdx, node->partOperators,
                                                         node->partCollations, &winstate->ss.ps);
    if (node->ordNumCols > 0)
        winstate->ordEqfunction = execTuplesMatchPrepare(scanDesc, node->ordNumCols,
                                                        node->ordColIdx, node->ordOperators,
                                                        node->ordCollations, &winstate->ss.ps);

    // Initialize window function state arrays
    numfuncs = winstate->numfuncs;
    numaggs = winstate->numaggs;
    econtext = winstate->ss.ps.ps_ExprContext;
    econtext->ecxt_aggvalues = (Datum *) palloc0(sizeof(Datum) * numfuncs);
    econtext->ecxt_aggnulls = (bool *) palloc0(sizeof(bool) * numfuncs);

    perfunc = (WindowStatePerFunc) palloc0(sizeof(WindowStatePerFuncData) * numfuncs);
    peragg = (WindowStatePerAgg) palloc0(sizeof(WindowStatePerAggData) * numaggs);
    winstate->perfunc = perfunc;
    winstate->peragg = peragg;

    // Process each window function (simplified loop)
    wfuncno = -1;
    aggno = -1;
    foreach(l, winstate->funcs) {
        WindowFuncExprState *wfuncstate = (WindowFuncExprState *) lfirst(l);
        WindowFunc *wfunc = wfuncstate->wfunc;
        WindowStatePerFunc perfuncstate;

        // Check for duplicates (simplified)
        int i;
        for (i = 0; i <= wfuncno; i++) {
            if (equal(wfunc, perfunc[i].wfunc) && !contain_volatile_functions((Node *) wfunc))
                break;
        }
        if (i <= wfuncno) {
            wfuncstate->wfuncno = i;
            continue;
        }

        // Set up new function state
        perfuncstate = &perfunc[++wfuncno];
        wfuncstate->wfuncno = wfuncno;

        // Initialize function properties
        perfuncstate->wfuncstate = wfuncstate;
        perfuncstate->wfunc = wfunc;
        perfuncstate->numArguments = list_length(wfuncstate->args);
        perfuncstate->winCollation = wfunc->inputcollid;

        get_typlenbyval(wfunc->wintype, &perfuncstate->resulttypeLen, &perfuncstate->resulttypeByVal);

        // Handle plain aggregates vs. true window functions
        perfuncstate->plain_agg = wfunc->winagg;
        if (wfunc->winagg) {
            // Set up aggregate processing
            perfuncstate->aggno = ++aggno;
            initialize_peragg(winstate, wfunc, &winstate->peragg[aggno]);
            winstate->peragg[aggno].wfuncno = wfuncno;
        } else {
            // Set up window function processing
            WindowObject winobj = makeNode(WindowObjectData);
            winobj->winstate = winstate;
            winobj->argstates = wfuncstate->args;
            winobj->localmem = NULL;
            perfuncstate->winobj = winobj;

            fmgr_info_cxt(wfunc->winfnoid, &perfuncstate->flinfo, econtext->ecxt_per_query_memory);
        }
    }

    // Update function counts and initialize aggregate WindowObject if needed
    winstate->numfuncs = wfuncno + 1;
    winstate->numaggs = aggno + 1;

    if (winstate->numaggs > 0) {
        WindowObject agg_winobj = makeNode(WindowObjectData);
        agg_winobj->winstate = winstate;
        agg_winobj->argstates = NIL;
        agg_winobj->localmem = NULL;
        agg_winobj->markptr = -1;
        agg_winobj->readptr = -1;
        winstate->agg_winobj = agg_winobj;
    }

    // Initialize frame boundary expressions
    winstate->startOffset = ExecInitExpr((Expr *) node->startOffset, (PlanState *) winstate);
    winstate->endOffset = ExecInitExpr((Expr *) node->endOffset, (PlanState *) winstate);

    // Set up in_range support functions for RANGE mode
    if (OidIsValid(node->startInRangeFunc))
        fmgr_info(node->startInRangeFunc, &winstate->startInRangeFunc);
    if (OidIsValid(node->endInRangeFunc))
        fmgr_info(node->endInRangeFunc, &winstate->endInRangeFunc);

    winstate->inRangeColl = node->inRangeColl;
    winstate->inRangeAsc = node->inRangeAsc;
    winstate->inRangeNullsFirst = node->inRangeNullsFirst;

    // Initialize execution state
    winstate->status = WINDOWAGG_RUN;
    winstate->all_first = true;
    winstate->partition_spooled = false;
    winstate->more_partitions = false;

    return winstate;
}
```