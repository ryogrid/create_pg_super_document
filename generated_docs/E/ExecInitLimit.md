# ExecInitLimit

## Location
[src/backend/executor/nodeLimit.c:447-533](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeLimit.c#L447-L533)

## Overview
ExecInitLimit initializes a Limit node's state structure and subplan, setting up expression evaluation, result type information, and WITH TIES comparison functionality if needed.

## Definition

```c
structure
	 */
	limitstate = makeNode(LimitState);
```
## Detailed Description
ExecInitLimit is responsible for setting up all necessary state and data structures for a Limit execution node. The function creates a LimitState structure, initializes the child plan, sets up expression contexts for evaluating LIMIT/OFFSET parameters, and configures result type information.

Key initialization tasks include:
- Creating the LimitState node and linking it to the plan tree
- Setting the execution function pointer to ExecLimit
- Initializing the outer (child) plan node
- Setting up expression evaluation for LIMIT and OFFSET parameters
- Configuring result tuple slot operations to match the child plan
- For WITH TIES: creating comparison infrastructure including an extra tuple slot for storing the boundary tuple and setting up equality functions for tie detection

The function ensures compatibility with the execution framework by properly initializing expression contexts, result type information, and plan state linkage.

## Parameters / Member Variables
- : Limit plan node containing the configuration and expressions
- : Executor state containing global execution context
- : Execution flags (EXEC_FLAG_MARK is not supported and asserted against)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates LimitState structure)
  - [ExecAssignExprContext](ExecAssignExprContext.md) (sets up expression evaluation context)  
  - [ExecInitNode](ExecInitNode.md) (initializes child plan recursively)
  - [ExecInitExpr](ExecInitExpr.md) (initializes LIMIT/OFFSET expressions)
  - [ExecInitResultTypeTL](ExecInitResultTypeTL.md) (sets up result tuple descriptor)
  - [ExecGetResultSlotOps](ExecGetResultSlotOps.md) (gets tuple slot operations)
  - [ExecGetResultType](ExecGetResultType.md) (gets child plan's result type)
  - [ExecInitExtraTupleSlot](ExecInitExtraTupleSlot.md) (creates slot for WITH TIES boundary tuple)
  - [execTuplesMatchPrepare](../e/execTuplesMatchPrepare.md) (prepares tuple comparison for WITH TIES)
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md) (as part of plan tree initialization)

## Notes and Other Information
- Does not support EXEC_FLAG_MARK execution flag (would be used for mark/restore functionality)
- Sets ps_ProjInfo to NULL since Limit nodes perform no projection
- WITH TIES support requires additional initialization of comparison infrastructure
- The limit/offset expressions are not evaluated during initialization since parameters may not be available yet
- [Result](../R/Result.md) slot operations are inherited from the child plan for efficiency
- Expression context is required even though Limit nodes don't use ExecQual or ExecProject

## Simplified Source

```c
LimitState *
ExecInitLimit(Limit *node, EState *estate, int eflags)
{
    LimitState *limitstate;
    Plan *outerPlan;

    // Validate execution flags - mark/restore not supported
    Assert(!(eflags & EXEC_FLAG_MARK));

    // Create and initialize state structure
    limitstate = makeNode(LimitState);
    limitstate->ps.plan = (Plan *) node;
    limitstate->ps.state = estate;
    limitstate->ps.ExecProcNode = ExecLimit;

    // Set initial execution state
    limitstate->lstate = LIMIT_INITIAL;

    // Set up expression context for limit/offset parameter evaluation
    ExecAssignExprContext(estate, &limitstate->ps);

    // Initialize child plan (outer plan)
    outerPlan = outerPlan(node);
    outerPlanState(limitstate) = ExecInitNode(outerPlan, estate, eflags);

    // Initialize LIMIT and OFFSET expressions
    limitstate->limitOffset = ExecInitExpr((Expr *) node->limitOffset,
                                          (PlanState *) limitstate);
    limitstate->limitCount = ExecInitExpr((Expr *) node->limitCount,
                                         (PlanState *) limitstate);
    limitstate->limitOption = node->limitOption;

    // Initialize result type (inherits from child plan)
    ExecInitResultTypeTL(&limitstate->ps);

    // Set up result slot operations to match child plan
    limitstate->ps.resultopsset = true;
    limitstate->ps.resultops = ExecGetResultSlotOps(outerPlanState(limitstate),
                                                   &limitstate->ps.resultopsfixed);

    // No projection needed for limit nodes
    limitstate->ps.ps_ProjInfo = NULL;

    // Initialize WITH TIES functionality if needed
    if (node->limitOption == LIMIT_OPTION_WITH_TIES)
    {
        TupleDesc desc;
        const TupleTableSlotOps *ops;

        // Get child plan's result type information
        desc = ExecGetResultType(outerPlanState(limitstate));
        ops = ExecGetResultSlotOps(outerPlanState(limitstate), NULL);

        // Create slot for storing boundary tuple
        limitstate->last_slot = ExecInitExtraTupleSlot(estate, desc, ops);

        // Set up tuple comparison function for tie detection
        limitstate->eqfunction = execTuplesMatchPrepare(desc,
                                                       node->uniqNumCols,
                                                       node->uniqColIdx,
                                                       node->uniqOperators,
                                                       node->uniqCollations,
                                                       &limitstate->ps);
    }

    return limitstate;
}
```