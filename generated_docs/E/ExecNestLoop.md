# ExecNestLoop

## Location
[src/backend/executor/nodeNestloop.c:60-261](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeNestloop.c#L60-L261)

## Overview
ExecNestLoop executes a nested loop join operation between outer and inner relations, returning tuples that satisfy join and other qualification conditions.

## Definition

```c
structure
	 */
	nlstate = makeNode(NestLoopState);
```
## Detailed Description
ExecNestLoop implements the core nested loop join algorithm in PostgreSQL's executor. It performs a nested iteration where for each tuple from the outer relation, it scans through all tuples in the inner relation to find matches based on join conditions.

The function operates in a continuous loop, maintaining state through the NestLoopState structure. When it needs a new outer tuple, it fetches one from the outer plan and resets the inner scan. For each outer tuple, it processes inner tuples until either a qualifying join tuple is found or the inner relation is exhausted.

The algorithm supports various join types including inner joins, left outer joins, and anti-joins. For outer joins, when no matching inner tuple is found, it generates a result tuple with null values for inner attributes. For anti-joins, it only returns outer tuples that have no matches in the inner relation.

The function also handles parameterized nested loops where outer tuple values are passed as parameters to the inner scan through the nestParams mechanism, enabling more efficient execution of correlated subqueries.

## Parameters / Member Variables
- : The PlanState node containing execution state information for the nested loop join

## Dependencies
- Functions called/Symbols referenced:
  - [ExecProcNode](ExecProcNode.md): Gets next tuple from outer/inner plan
  - [ExecQual](ExecQual.md): Evaluates join and other qualification expressions  
  - [ExecProject](ExecProject.md): Projects result tuple using projection info
  - [ExecReScan](ExecReScan.md): Rescans inner plan when starting with new outer tuple
  - ResetExprContext: Resets per-tuple expression evaluation memory
  - TupIsNull: Checks if tuple slot is null
  - [slot_getattr](../s/slot_getattr.md): Extracts attribute value from tuple slot
  - [bms_add_member](../b/bms_add_member.md): Adds parameter to changed parameter bitmap
  - InstrCountFiltered1/InstrCountFiltered2: Updates instrumentation counters
- Called from (representative examples):
  - [ExecInitNestLoop](ExecInitNestLoop.md): During node initialization and execution

## Notes and Other Information
- Uses ENL1_printf debug macros for tracing execution flow
- Handles CHECK_FOR_INTERRUPTS() to allow query cancellation
- Maintains nl_NeedNewOuter and nl_MatchedOuter flags to track join state
- Supports single_match optimization for semi-joins
- Uses ecxt_outertuple and ecxt_innertuple in expression context for qualification evaluation
- Memory management through ResetExprContext() prevents memory leaks in long-running joins
- Supports instrumentation for monitoring filtered tuple counts

## Simplified Source

```c
static TupleTableSlot *ExecNestLoop(PlanState *pstate)
{
    NestLoopState *node = castNode(NestLoopState, pstate);
    NestLoop *nl = (NestLoop *) node->js.ps.plan;
    PlanState *outerPlan = outerPlanState(node);
    PlanState *innerPlan = innerPlanState(node);
    ExprContext *econtext = node->js.ps.ps_ExprContext;

    CHECK_FOR_INTERRUPTS();

    // Reset per-tuple memory context
    ResetExprContext(econtext);

    // Main nested loop iteration
    for (;;)
    {
        // Get new outer tuple if needed
        if (node->nl_NeedNewOuter)
        {
            TupleTableSlot *outerTupleSlot = ExecProcNode(outerPlan);

            // No more outer tuples - join complete
            if (TupIsNull(outerTupleSlot))
                return NULL;

            // Set up new outer tuple context
            econtext->ecxt_outertuple = outerTupleSlot;
            node->nl_NeedNewOuter = false;
            node->nl_MatchedOuter = false;

            // Pass outer tuple values as parameters to inner scan
            foreach(lc, nl->nestParams)
            {
                NestLoopParam *nlp = (NestLoopParam *) lfirst(lc);
                ParamExecData *prm = &(econtext->ecxt_param_exec_vals[nlp->paramno]);

                prm->value = slot_getattr(outerTupleSlot, nlp->paramval->varattno, &(prm->isnull));
                innerPlan->chgParam = bms_add_member(innerPlan->chgParam, nlp->paramno);
            }

            // Reset inner scan for new outer tuple
            ExecReScan(innerPlan);
        }

        // Get next inner tuple
        TupleTableSlot *innerTupleSlot = ExecProcNode(innerPlan);
        econtext->ecxt_innertuple = innerTupleSlot;

        if (TupIsNull(innerTupleSlot))
        {
            // No more inner tuples for this outer tuple
            node->nl_NeedNewOuter = true;

            // Handle outer join - return outer tuple with null inner values
            if (!node->nl_MatchedOuter &&
                (node->js.jointype == JOIN_LEFT || node->js.jointype == JOIN_ANTI))
            {
                econtext->ecxt_innertuple = node->nl_NullInnerTupleSlot;

                if (node->js.ps.qual == NULL || ExecQual(node->js.ps.qual, econtext))
                    return ExecProject(node->js.ps.ps_ProjInfo);
            }
            continue;
        }

        // Test join and other qualification conditions
        if (ExecQual(node->js.joinqual, econtext))
        {
            node->nl_MatchedOuter = true;

            // Anti-join: don't return matched tuples
            if (node->js.jointype == JOIN_ANTI)
            {
                node->nl_NeedNewOuter = true;
                continue;
            }

            // Single-match optimization for semi-joins
            if (node->js.single_match)
                node->nl_NeedNewOuter = true;

            // Check other qualifications and project result
            if (node->js.ps.qual == NULL || ExecQual(node->js.ps.qual, econtext))
                return ExecProject(node->js.ps.ps_ProjInfo);
        }

        // Tuple didn't qualify - reset and continue
        ResetExprContext(econtext);
    }
}
```