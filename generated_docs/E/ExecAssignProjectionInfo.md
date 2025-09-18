# ExecAssignProjectionInfo

## Location
src/backend/executor/execUtils.c: 538 - 557

## Overview
Creates and assigns projection information to a plan state node by building projection infrastructure from the node's target list, enabling efficient tuple projection operations during execution.

## Definition
```c
void ExecAssignProjectionInfo(PlanState *planstate, TupleDesc inputDesc)
```

## Detailed Description
ExecAssignProjectionInfo is a convenience wrapper around ExecBuildProjectionInfo that constructs and assigns projection information to a plan state node. The function builds a ProjectionInfo structure from the plan's target list, which contains the expressions that define what columns/values should be included in the output tuples.

The projection information enables the executor to efficiently transform input tuples into output tuples by evaluating the target list expressions. This is a fundamental operation in PostgreSQL's execution engine, used for selecting specific columns, computing expressions, and reshaping tuple data.

The function automatically uses the planstate's expression context, result tuple slot, and other execution state to build the projection infrastructure.

## Parameters / Member Variables
- `planstate`: Pointer to the PlanState structure that will receive the projection information
- `inputDesc`: Tuple descriptor for input tuples (required for relation-scan nodes, can be NULL for upper-level nodes)

## Dependencies
- Functions called/Symbols referenced:
  - [ExecBuildProjectionInfo](ExecBuildProjectionInfo.md) (builds the actual projection infrastructure)
- Called from (representative examples):
  - [ExecConditionalAssignProjectionInfo](ExecConditionalAssignProjectionInfo.md) (execUtils.c:580)
  - [ExecInitAgg](ExecInitAgg.md) (nodeAgg.c:3351)
  - [ExecInitGroup](ExecInitGroup.md) (nodeGroup.c:198)
  - [ExecInitHashJoin](ExecInitHashJoin.md) (nodeHashjoin.c:763)
  - ExecInitMergeJoin (nodeMergejoin.c:1525)
  - Various executor node initialization functions

## Notes and Other Information
- This function simplifies projection setup by automatically using the planstate's existing execution context
- The inputDesc parameter should be provided for relation-scan nodes but can be NULL for upper-level processing nodes
- Essential for any plan node that needs to project or transform tuple data
- The resulting ProjectionInfo structure is stored in planstate->ps_ProjInfo
- Used extensively during executor node initialization to set up tuple projection capabilities