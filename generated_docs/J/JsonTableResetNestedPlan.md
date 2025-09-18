# JsonTableResetNestedPlan

## Location
src/backend/utils/adt/jsonpath_exec.c: 4382 - 4410

## Overview
JsonTableResetNestedPlan re-evaluates the row pattern of nested JSON table plans using updated parent row context, enabling hierarchical path evaluation in JSON table operations.

## Definition


## Detailed Description
This function recursively resets nested JSON table plans to re-evaluate their row patterns based on updated parent row data. It handles two types of plans: JsonTablePathScan plans are reset by calling JsonTableResetRowPattern with the parent's current row value, while JsonTableSiblingJoin plans are handled by recursively resetting both left and right child plans. The function ensures that nested paths are properly re-evaluated when their parent context changes, maintaining the hierarchical relationship between different levels of JSON path evaluation.

## Parameters / Member Variables
- : Pointer to JsonTablePlanState structure representing the nested plan to be reset. Must have a non-NULL parent (assertion enforced)

## Dependencies
- Functions called/Symbols referenced:
  - JsonTableResetRowPattern (resets row pattern evaluation for path scan plans)
  - JsonTableResetNestedPlan (recursive calls for sibling join plans)
  - IsA (PostgreSQL macro for type checking)
  - Assert (assertion macro for debugging)
- Called from (representative examples):
  - JsonTablePlanScanNextRow (when processing nested plans with new parent rows)
  - JsonTableResetNestedPlan (recursive calls for sibling join plans)

## Notes and Other Information
- This function only operates on child plans (planstate->parent must not be NULL)
- For JsonTablePathScan plans, the reset only occurs if the parent has a non-null current row
- For JsonTableSiblingJoin plans, both left and right child plans are recursively reset
- The function implements a depth-first traversal pattern for resetting nested plan hierarchies
- Child nested plans of a JsonTablePathScan are reset implicitly when JsonTablePlanNextRow() is subsequently called
- The function is essential for maintaining proper parent-child relationships in complex JSON table expressions with multiple nesting levels