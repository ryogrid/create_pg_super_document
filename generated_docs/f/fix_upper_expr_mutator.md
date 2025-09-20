# fix_upper_expr_mutator

## Location
[src/backend/optimizer/plan/setrefs.c:3214-3316](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L3214-L3316)

## Overview
A recursive expression tree mutator that transforms variable references in upper-level plan node expressions by mapping them to subplan target list entries, with specialized handling for aggregates and placeholder variables.

## Definition

```c
static Node *
fix_upper_expr_mutator(Node *node, fix_upper_expr_context *context)
```
## Detailed Description
The  function is the core worker function called by  to perform the actual expression tree transformation. It recursively traverses expression trees and replaces variable references with appropriate references to the subplan's target list outputs. The function is specifically designed for upper-level (non-join) plan nodes.

Key operations performed by this function include:

1. **Variable Reference Resolution**: For  nodes, it searches the subplan's indexed target list to find matching variables and replaces them with new variable references using the specified variable number.

2. **PlaceHolderVar Processing**: It handles  nodes by first attempting to find them in the subplan target list, and if not found, recursively processing their contained expressions.

3. **Complex Expression Matching**: When the target list contains non-variable expressions, it attempts to match entire subexpressions to avoid recomputation.

4. **Aggregate Replacement**: For  nodes, it checks if they can be replaced with parameters from min/max optimization, which is an important query optimization technique.

5. **Special Node Handling**: It provides specialized processing for  nodes and  nodes.

The function ensures that all variable references are properly mapped and includes error checking to guarantee that all variables can be resolved.

## Parameters / Member Variables
- : The expression node currently being processed and potentially transformed
- : Context structure containing transformation state including:
  - : PlannerInfo structure with global planner information
  - : Indexed target list from the subplan being referenced
  - : Variable number to assign to new variable references
  - : Range table offset for adjusting relation numbers
  - : Nulling-resilient matching specification
  - : Estimated execution count for cost-based decisions

## Dependencies
- Functions called/Symbols referenced:
  - [search_indexed_tlist_for_var](../s/search_indexed_tlist_for_var.md)
  - [search_indexed_tlist_for_phv](../s/search_indexed_tlist_for_phv.md)
  - [search_indexed_tlist_for_non_var](../s/search_indexed_tlist_for_non_var.md)
  - [fix_param_node](fix_param_node.md)
  - [find_minmax_agg_replacement_param](find_minmax_agg_replacement_param.md)
  - copyObject
  - [fix_alternative_subplan](fix_alternative_subplan.md)
  - [fix_expr_common](fix_expr_common.md)
  - expression_tree_mutator
- Called from (representative examples):
  - fix_scan_list
  - [fix_upper_expr](fix_upper_expr.md)
  - [fix_upper_expr_mutator](fix_upper_expr_mutator.md) (recursive calls)

## Notes and Other Information
- This function is the actual workhorse behind  and performs the detailed tree transformation
- It includes comprehensive error checking with  when variables cannot be found in the target list
- The function has special optimization logic for aggregate expressions that can be replaced with min/max parameters
- It supports complex expression matching to avoid recomputing expressions already calculated by the subplan
- The function is static and internal to the setrefs.c module
- Uses PostgreSQL's expression tree mutator framework for efficient and safe tree traversal
- Includes paranoia-based copying of objects to prevent unintended side effects
- The XXX comment indicates areas where the developers noted potential future improvements or clarifications needed