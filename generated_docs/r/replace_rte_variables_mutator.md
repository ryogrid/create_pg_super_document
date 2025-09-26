# replace_rte_variables_mutator

## Location
[src/backend/rewrite/rewriteManip.c:1393-1488](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L1393-L1488)

## Overview
The core mutator function that implements the recursive tree walking and variable replacement logic for replace_rte_variables, handling different node types and maintaining sublevel tracking.

## Definition

```c
typedef struct
{
	int			target_varno;	/* RTE index to search for */
	int			sublevels_up;	/* (current) nesting depth */
	const AttrMap *attno_map;	/* map array for user attnos */
	Oid			to_rowtype;		/* change whole-row Vars to this type */
	bool	   *found_whole_row;	/* output flag */
} map_variable_attnos_context;
```
## Detailed Description
This function serves as the recursive workhorse for replace_rte_variables, implementing the actual tree traversal and node replacement logic. It handles several specific node types with special processing:

1. **Var nodes**: Checks if the variable matches the target RTE (by varno and varlevelsup), and if so, invokes the callback to perform the replacement. It also tracks whether SubLinks are being inserted during replacement.

2. **CurrentOfExpr nodes**: Detects WHERE CURRENT OF expressions that apply to views and raises an error since this feature is not yet implemented in PostgreSQL.

3. **Query nodes**: Handles subqueries by incrementing sublevels_up, managing SubLink tracking across sublevel boundaries, and recursively processing the query tree.

4. **All other nodes**: Uses the standard expression_tree_mutator for recursive processing.

The function carefully manages the SubLink tracking state, preserving and restoring context across recursive calls to maintain accurate hasSubLinks information.

## Parameters
- `node`: The current node being processed in the expression tree
- `context`: Contains callback function, target RTE information, sublevel tracking, and SubLink state

## Dependencies
- Functions called/Symbols referenced:
  - [checkExprHasSubLink](../c/checkExprHasSubLink.md)
  - query_tree_mutator
  - expression_tree_mutator
  - [replace_rte_variables_context](replace_rte_variables_context.md) (struct)
  - [CurrentOfExpr](../C/CurrentOfExpr.md) (node type)
- Called from (representative examples):
  - [replace_rte_variables](replace_rte_variables.md)
  - [pullup_replace_vars_callback](../p/pullup_replace_vars_callback.md)
  - [ReplaceVarsFromTargetList_callback](../R/ReplaceVarsFromTargetList_callback.md)
  - [replace_rte_variables_mutator](replace_rte_variables_mutator.md) (recursive calls)

## Notes and Other Information
- The function is publicly exposed (unlike typical mutator functions) because callbacks often need to recurse directly to it on sub-expressions
- WHERE CURRENT OF on views is explicitly not supported and will raise an error
- [SubLink](../S/SubLink.md) tracking is carefully managed across recursive calls to ensure accurate query metadata
- The function handles both planned and unplanned subquery contexts appropriately
- Recursive calls to itself occur when processing Query nodes and through expression_tree_mutator for general expression processing