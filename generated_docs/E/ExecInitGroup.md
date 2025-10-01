# ExecInitGroup

## Location
[src/backend/executor/nodeGroup.c:161-225](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeGroup.c#L161-L225)

## Overview
ExecInitGroup initializes the runtime state for a Group plan node, setting up the necessary data structures, expression contexts, and child node connections for GROUP BY execution.

## Definition

```c
structure
	 */
	grpstate = makeNode(GroupState);
```
## Detailed Description
ExecInitGroup performs comprehensive initialization of a GroupState node during query plan startup. It creates the execution state structure, establishes parent-child relationships in the plan tree, initializes expression evaluation contexts, and sets up tuple processing infrastructure.

Key initialization tasks include:
- Creating and configuring the GroupState structure
- Initializing the child (outer) plan node recursively
- Setting up scan slots and result slots with appropriate tuple operations
- Initializing projection info for result tuple formation
- Compiling HAVING clause expressions for runtime evaluation  
- Precomputing tuple comparison functions for efficient group boundary detection

The function also validates that unsupported execution flags (backward scan, mark/restore) are not requested, as Group nodes don't support these operations.

## Parameters / Member Variables
- : The Group plan node from the planner containing grouping specification
- : The execution state containing global query execution context
- : Execution flags controlling scan behavior and optimization hints

## Dependencies
- Functions called/Symbols referenced:
  - [GroupState](../G/GroupState.md) (state structure creation)
  - [Group](../G/Group.md) (plan node type)
  - [TupleTableSlotOps](../T/TupleTableSlotOps.md) (slot operations)
  - EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK (unsupported flags)
  - makeNode (memory allocation)
  - [ExecGroup](ExecGroup.md) (assigned as execution function)
  - [ExecAssignExprContext](ExecAssignExprContext.md) (expression context setup)
  - [ExecInitNode](ExecInitNode.md) (child initialization)
  - outerPlan, outerPlanState (child plan access)
  - [ExecGetResultSlotOps](ExecGetResultSlotOps.md) (slot operations determination)
  - [ExecCreateScanSlotFromOuterPlan](ExecCreateScanSlotFromOuterPlan.md) (scan slot creation)
  - [ExecInitResultTupleSlotTL](ExecInitResultTupleSlotTL.md) (result slot initialization)
  - [ExecAssignProjectionInfo](ExecAssignProjectionInfo.md) (projection setup)
  - [ExecInitQual](ExecInitQual.md) (HAVING clause compilation)
  - [execTuplesMatchPrepare](../e/execTuplesMatchPrepare.md) (group comparison setup)
  - [ExecGetResultType](ExecGetResultType.md) (type information)
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md) (during plan tree initialization)

## Notes and Other Information
- Validates that backward scanning and mark/restore are not requested
- Uses virtual tuple slots for result tuples to optimize memory usage
- Precomputes tuple comparison functions using execTuplesMatchPrepare for efficient group detection
- The eqfunction field stores compiled comparison logic for the grouping columns
- Child node initialization happens before parent node setup to establish proper dependencies
- Expression contexts are essential for qual and projection evaluation during execution

## Simplified Source

```c
GroupState *
ExecInitGroup(Group *node, EState *estate, int eflags)
{
    // Create and initialize the Group state structure
    GroupState *grpstate = makeNode(GroupState);
    grpstate->ss.ps.plan = (Plan *) node;
    grpstate->ss.ps.state = estate;
    grpstate->ss.ps.ExecProcNode = ExecGroup;
    grpstate->grp_done = false;

    // Create expression context
    ExecAssignExprContext(estate, &grpstate->ss.ps);

    // Initialize the outer child plan
    outerPlanState(grpstate) = ExecInitNode(outerPlan(node), estate, eflags);

    // Initialize scan slot from outer plan
    const TupleTableSlotOps *tts_ops = ExecGetResultSlotOps(outerPlanState(&grpstate->ss), NULL);
    ExecCreateScanSlotFromOuterPlan(estate, &grpstate->ss, tts_ops);

    // Initialize result slot and projection
    ExecInitResultTupleSlotTL(&grpstate->ss.ps, &TTSOpsVirtual);
    ExecAssignProjectionInfo(&grpstate->ss.ps, NULL);

    // Initialize qualification expressions
    grpstate->ss.ps.qual = ExecInitQual(node->plan.qual, (PlanState *) grpstate);

    // Precompute tuple comparison functions for group detection
    grpstate->eqfunction = execTuplesMatchPrepare(ExecGetResultType(outerPlanState(grpstate)),
                                                 node->numCols,
                                                 node->grpColIdx,
                                                 node->grpOperators,
                                                 node->grpCollations,
                                                 &grpstate->ss.ps);

    return grpstate;
}
```