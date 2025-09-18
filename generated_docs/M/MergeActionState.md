# MergeActionState

## Location
src/include/nodes/execnodes.h: 423 - 431

## Overview
MergeActionState holds the executor state for a MERGE action, managing the execution of individual WHEN clauses within MERGE statements.

## Definition


## Detailed Description
MergeActionState represents the execution state for individual actions within a MERGE statement. MERGE statements can have multiple WHEN MATCHED and WHEN NOT MATCHED clauses, each with their own conditions and actions. This structure maintains the runtime state needed to evaluate and execute one specific MERGE action.

The structure contains references to the associated parser node, projection information for computing target list values, and expression state for evaluating the WHEN clause conditions.

## Parameters / Member Variables
- : NodeTag identifier for the structure type
- : Pointer to the associated MergeAction node from the parse tree
- : ProjectionInfo for projecting the action's targetlist for this relation
- : Expression state for evaluating WHEN [NOT] MATCHED AND conditions

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag
  - MergeAction
  - [ProjectionInfo](../P/ProjectionInfo.md)
  - ExprState
- Called from (representative examples):
  - [ExecMergeMatched](../E/ExecMergeMatched.md)
  - [ExecMergeNotMatched](../E/ExecMergeNotMatched.md)
  - [ExecInitMerge](../E/ExecInitMerge.md)
  - [ExecEvalMergeSupportFunc](../E/ExecEvalMergeSupportFunc.md)

## Notes and Other Information
- Core component of PostgreSQL's MERGE statement execution engine
- Each WHEN clause in a MERGE statement has its own MergeActionState
- Supports both WHEN MATCHED and WHEN NOT MATCHED scenarios
- The mas_whenqual allows for conditional execution based on AND conditions in WHEN clauses
- Works with ModifyTableState to coordinate multiple merge actions
- Essential for implementing SQL standard MERGE functionality in PostgreSQL