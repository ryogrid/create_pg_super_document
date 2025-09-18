# fix_scan_expr_mutator

## Location
src/backend/optimizer/plan/setrefs.c: 2195 - 2260

## Overview
A tree mutator function that recursively processes expression nodes to adjust variable references, replace parameters, and handle special node types during scan-level expression fixing.

## Definition
```c
static Node *fix_scan_expr_mutator(Node *node, fix_scan_expr_context *context)
```

## Detailed Description
The fix_scan_expr_mutator function is the recursive workhorse of the scan expression fixing process. It implements a tree mutator pattern that traverses expression trees and applies various transformations based on node type:

1. **Variable Processing**: For Var nodes, it creates a copy and adjusts both varno and varnosyn by the rtoffset, while ensuring proper handling of special variable types (INNER_VAR, OUTER_VAR, etc.)

2. **Parameter Handling**: Delegates Param node processing to fix_param_node for proper parameter substitution

3. **Aggregate Reference Replacement**: For Aggref nodes, it checks if the aggregate should be replaced with a parameter using find_minmax_agg_replacement_param

4. **Current Of Expression**: Handles CurrentOfExpr nodes by copying and adjusting the cursor variable number

5. **PlaceHolderVar Expansion**: Recursively processes the contained expression (phexpr) for PlaceHolderVar nodes

6. **Alternative SubPlan Resolution**: Processes AlternativeSubPlan nodes by first choosing the best alternative, then recursively processing the result

7. **General Expression Processing**: Applies common expression fixes and recursively processes child nodes using expression_tree_mutator

The function maintains the tree structure while creating copies where necessary to avoid modifying the original tree when transformations are required.

## Parameters / Member Variables
- `node`: The expression node to be processed and potentially modified
- `context`: fix_scan_expr_context structure containing root PlannerInfo, rtoffset, and execution count information

## Dependencies
- Functions called/Symbols referenced:
  - [copyVar](../c/copyVar.md)
  - [fix_param_node](fix_param_node.md)
  - [find_minmax_agg_replacement_param](find_minmax_agg_replacement_param.md)
  - copyObject
  - [fix_alternative_subplan](fix_alternative_subplan.md)
  - [fix_expr_common](fix_expr_common.md)
  - expression_tree_mutator
- Called from (representative examples):
  - fix_scan_list
  - [fix_scan_expr](fix_scan_expr.md)
  - [fix_scan_expr_mutator](fix_scan_expr_mutator.md) (recursive calls)

## Notes and Other Information
- This function implements the copy-and-modify approach used when transformations are necessary, as opposed to the in-place modification used by fix_scan_expr_walker
- Contains multiple assertions to validate that special variable types (INNER_VAR, OUTER_VAR, ROWID_VAR) are not present at scan level
- The recursive nature allows it to handle arbitrarily complex expression trees
- For PlaceHolderVars at scan level, it always evaluates the contained expression rather than preserving the placeholder
- Located in src/backend/optimizer/plan/setrefs.c:2195-2260