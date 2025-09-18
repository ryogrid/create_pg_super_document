# planstate_walk_subplans

## Location
src/backend/nodes/nodeFuncs.c: 4760 - 4781

## Overview
A specialized helper function that walks through a list of SubPlan or initPlan nodes during planstate tree traversal.

## Definition


## Detailed Description
The  function is a static helper within the planstate tree walking infrastructure that handles traversal of SubPlan lists. SubPlans represent subqueries that have been planned and are embedded within a larger query plan. This function iterates through a list of SubPlanState nodes and recursively walks each subplan's planstate using the  macro.

This function is used internally by the larger planstate tree walker to handle both regular SubPlans (subqueries in expressions) and initPlans (subqueries that can be evaluated once at query startup). The function ensures that all nested plan structures are visited during tree traversal operations.

## Parameters / Member Variables
- : List of SubPlanState nodes to be traversed
- : Callback function that defines the walking behavior for each visited planstate node
- : Opaque context pointer passed through to the walker callback

## Dependencies
- Functions called/Symbols referenced:
  - lfirst_node (macro for extracting typed node from list cell)
  - PSWALK (macro for recursive planstate walking)
  - [SubPlanState](../S/SubPlanState.md) (node type for subplan execution state)
- Called from (representative examples):
  - PSWALK macro (indirectly when walking nodes that contain subplan lists)
  - Various planstate node walking operations

## Notes and Other Information
- Returns boolean indicating whether the walk should terminate early (true) or continue (false)
- Static function, only used internally within the nodeFuncs.c file
- Handles both SubPlans (correlated subqueries) and initPlans (uncorrelated subqueries that can be pre-evaluated)
- Part of PostgreSQL's execution plan traversal infrastructure used for plan tree analysis and transformation
- The function terminates early if any subplan walk returns true, following the standard early termination pattern
- Located in src/backend/nodes/nodeFuncs.c:4760-4781