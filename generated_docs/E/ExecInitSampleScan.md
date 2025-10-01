# ExecInitSampleScan

## Location
[src/backend/executor/nodeSamplescan.c:93-178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSamplescan.c#L93-L178)

## Overview
ExecInitSampleScan initializes a sample scan executor node, setting up all necessary state, expressions, and table sampling infrastructure required for executing TABLESAMPLE operations.

## Definition

```c
SampleScanState *ExecInitSampleScan(SampleScan *node, EState *estate, int eflags)
```
## Detailed Description
ExecInitSampleScan is the initialization function for sample scan executor nodes in PostgreSQL. It performs comprehensive setup including creating the SampleScanState structure, opening the scan relation, initializing expression contexts and projections, setting up table sampling parameters, and configuring the specific table sampling method handler. The function handles both cases where a REPEATABLE clause is specified and where a random seed needs to be generated. It defers the actual BeginSampleScan call until later when parameters can be properly evaluated. The initialization follows PostgreSQL's standard executor node pattern while adding sample-specific setup like TSM routine initialization.

## Parameters / Member Variables
- `node`: Pointer to the SampleScan plan node containing the sampling specification and target relation
- `estate`: Pointer to the execution state containing transaction context and execution parameters
- `eflags`: Execution flags controlling initialization behavior (e.g., EXEC_FLAG_EXPLAIN_ONLY)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [ExecAssignExprContext](ExecAssignExprContext.md)
  - [ExecOpenScanRelation](ExecOpenScanRelation.md)
  - [ExecInitScanTupleSlot](ExecInitScanTupleSlot.md)
  - [ExecInitResultTypeTL](ExecInitResultTypeTL.md)
  - [ExecAssignScanProjectionInfo](ExecAssignScanProjectionInfo.md)
  - [ExecInitQual](ExecInitQual.md)
  - [ExecInitExprList](ExecInitExprList.md)
  - [ExecInitExpr](ExecInitExpr.md)
  - [pg_prng_uint32](../p/pg_prng_uint32.md)
  - [GetTsmRoutine](../G/GetTsmRoutine.md)
  - outerPlan/innerPlan (macros)
  - [table_slot_callbacks](../t/table_slot_callbacks.md)
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md)

## Notes and Other Information
- Returns a fully initialized SampleScanState ready for execution
- Asserts that sample scans have no child plans (outerPlan/innerPlan must be NULL)
- Generates a random seed automatically if no REPEATABLE clause is specified
- Sets up the table sampling method (TSM) routine but defers BeginSampleScan until parameters are evaluable
- The begun flag is initialized to false to trigger proper initialization on first tuple request
- Handles all standard executor node initialization including expression contexts, projections, and result types
- Part of the executor node interface that enables TABLESAMPLE clauses to work in SQL queries

## Simplified Source

```c
SampleScanState *
ExecInitSampleScan(SampleScan *node, EState *estate, int eflags)
{
    SampleScanState *scanstate;
    TableSampleClause *tsc = node->tablesample;
    TsmRoutine *tsm;

    // Validate no child plans
    Assert(outerPlan(node) == NULL);
    Assert(innerPlan(node) == NULL);

    // Create and initialize state structure
    scanstate = makeNode(SampleScanState);
    scanstate->ss.ps.plan = (Plan *) node;
    scanstate->ss.ps.state = estate;
    scanstate->ss.ps.ExecProcNode = ExecSampleScan;

    // Create expression context
    ExecAssignExprContext(estate, &scanstate->ss.ps);

    // Open the scan relation
    scanstate->ss.ss_currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid, eflags);
    scanstate->ss.ss_currentScanDesc = NULL;

    // Initialize scan tuple slot
    ExecInitScanTupleSlot(estate, &scanstate->ss,
                         RelationGetDescr(scanstate->ss.ss_currentRelation),
                         table_slot_callbacks(scanstate->ss.ss_currentRelation));

    // Initialize result handling
    ExecInitResultTypeTL(&scanstate->ss.ps);
    ExecAssignScanProjectionInfo(&scanstate->ss);

    // Initialize expressions
    scanstate->ss.ps.qual = ExecInitQual(node->scan.plan.qual, (PlanState *) scanstate);
    scanstate->args = ExecInitExprList(tsc->args, (PlanState *) scanstate);
    scanstate->repeatable = ExecInitExpr(tsc->repeatable, (PlanState *) scanstate);

    // Generate random seed if no REPEATABLE clause
    if (tsc->repeatable == NULL)
        scanstate->seed = pg_prng_uint32(&pg_global_prng_state);

    // Initialize table sampling method
    tsm = GetTsmRoutine(tsc->tsmhandler);
    scanstate->tsmroutine = tsm;
    scanstate->tsm_state = NULL;

    if (tsm->InitSampleScan)
        tsm->InitSampleScan(scanstate, eflags);

    // Defer BeginSampleScan until parameters can be evaluated
    scanstate->begun = false;

    return scanstate;
}
```