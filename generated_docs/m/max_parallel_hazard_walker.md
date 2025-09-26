# max_parallel_hazard_walker

## Location
[src/backend/optimizer/util/clauses.c:829-992](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L829-L992)

## Overview
The core expression tree walker function that recursively traverses PostgreSQL expression trees to identify parallel-unsafe or parallel-restricted constructs, implementing the detailed logic for parallel safety analysis.

## Definition

```c
structs
	 * anywhere in the tree.
	 */
	else if (IsA(node, Query))
	{
		Query	   *query = (Query *) node;

		/* SELECT FOR UPDATE/SHARE must be treated as unsafe */
		if (query->rowMarks != NULL)
		{
			context->max_hazard = PROPARALLEL_UNSAFE;
			return true;
		}

		/* Recurse into subselects */
		return query_tree_walker(query,
								 max_parallel_hazard_walker,
								 context, 0);
	}

	/* Recurse to check arguments */
	return expression_tree_walker(node,
								  max_parallel_hazard_walker,
								  context);
```
## Detailed Description
The  function is the workhorse of PostgreSQL's parallel safety analysis system. It implements a recursive tree walker that examines each node in an expression tree to determine whether it contains constructs that would prevent or restrict parallel execution.

The function handles numerous specific node types with different parallel safety characteristics:

**Function Calls**: Uses  with  to evaluate the parallel safety of function calls within the node.

**Specific Node Type Handling**:
- : Treated as parallel-restricted due to potential domain constraints containing unsafe functions
- : Marked as parallel-unsafe since sequence operations can't be safely parallelized  
- : Considered parallel-restricted because input row ordering determinism cannot be guaranteed across workers
- : Treated as parallel-restricted when encountered during restricted scans
- : Transparent wrapper - the walker recurses into the underlying clause
- : Handles subqueries, marking SELECT FOR UPDATE/SHARE as unsafe and recursing into subselects

**Parameter Handling**: Complex logic for  nodes that treats external parameters as safe, but restricts execution parameters unless they're in the safe parameter list.

**SubPlan Handling**: Special logic that temporarily adds SubPlan output parameter IDs to the safe parameter list when examining the testexpr, allowing more precise parallel safety analysis.

The function uses PostgreSQL's standard tree walking infrastructure ( and ) to ensure complete traversal while allowing early termination when unsafe constructs are found.

## Parameters / Member Variables
- : The current expression tree node being examined for parallel safety
- : Context structure tracking the maximum hazard level found and safe parameter IDs

## Dependencies
- Functions called/Symbols referenced:
  - [check_functions_in_node](../c/check_functions_in_node.md)
  - [max_parallel_hazard_checker](max_parallel_hazard_checker.md)
  - [max_parallel_hazard_test](max_parallel_hazard_test.md)
  - expression_tree_walker
  - query_tree_walker
  - [list_concat_copy](../l/list_concat_copy.md)
  - [list_free](../l/list_free.md)
  - [list_member_int](../l/list_member_int.md)
- Called from (representative examples):
  - [max_parallel_hazard](max_parallel_hazard.md)
  - [is_parallel_safe](../i/is_parallel_safe.md)
  - [max_parallel_hazard_walker](max_parallel_hazard_walker.md) (recursive calls)

## Notes and Other Information
- This is a static function that serves as the core implementation for all parallel safety checking
- The function implements early termination - it stops traversal as soon as an unacceptable hazard level is encountered
- Window functions are conservatively treated as parallel-restricted due to potential non-deterministic ordering across workers
- The SubPlan parameter handling demonstrates sophisticated context management, temporarily modifying the safe parameter list during subtree traversal
- SELECT FOR UPDATE/SHARE queries are immediately marked as unsafe without further analysis
- The function handles both completely unplanned trees (with Query nodes) and planned expression trees
- Located in src/backend/optimizer/util/clauses.c:829-992