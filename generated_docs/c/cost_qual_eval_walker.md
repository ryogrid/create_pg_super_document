# cost_qual_eval_walker

## Location
[src/backend/optimizer/path/costsize.c:4683-4964](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L4683-L4964)

## Overview
A recursive tree-walking function that computes detailed execution costs for individual expression nodes in qualification clauses.

## Definition

```c
struct equivalence to treat these all alike */
		set_opfuncid((OpExpr *) node);
```
## Detailed Description
The  function is the core workhorse of PostgreSQL's qualification cost estimation system. It performs a depth-first traversal of expression trees, accumulating execution costs for each node type based on their computational complexity and execution characteristics.

The function implements sophisticated cost modeling for various expression types:

**RestrictInfo Caching**: Uses eval_cost field to cache computed costs, avoiding redundant calculations for the same expressions. Handles OR clauses and pseudoconstant expressions specially.

**Function/Operator Costs**: Charges actual execution costs from pg_proc.procost for functions and operators, multiplied by cpu_operator_cost. Handles complex cases like ScalarArrayOpExpr with both hashed and linear search strategies.

**Special Node Types**: Provides specialized handling for aggregates (zero cost as they're handled at plan node level), type coercion, array operations, row comparisons, and subplans.

**Expression Tree Traversal**: Uses expression_tree_walker for recursive traversal while implementing custom logic for nodes that shouldn't recurse (like aggregates, subplans, and placeholders).

The function distinguishes between startup costs (paid once) and per-tuple costs (paid for each row), enabling accurate cost modeling for different execution contexts.

## Parameters / Member Variables
- : Expression node being evaluated for cost estimation
- : Cost evaluation context containing accumulated costs and planner information

## Dependencies
- Functions called/Symbols referenced:
  - [add_function_cost](../a/add_function_cost.md)
  - set_opfuncid
  - set_sa_opfuncid
  - [estimate_array_length](../e/estimate_array_length.md)
  - [cost_qual_eval_node](cost_qual_eval_node.md)
  - [getTypeInputInfo](../g/getTypeInputInfo.md)
  - [getTypeOutputInfo](../g/getTypeOutputInfo.md)
  - [get_opcode](../g/get_opcode.md)
  - expression_tree_walker
  - cost_qual_eval_context (struct)
  - Various expression node types (FuncExpr, OpExpr, ScalarArrayOpExpr, etc.)
- Called from (representative examples):
  - [cost_qual_eval](cost_qual_eval.md)
  - [cost_qual_eval_node](cost_qual_eval_node.md)
  - [cost_qual_eval_walker](cost_qual_eval_walker.md) (recursive calls)

## Notes and Other Information
- Static function within costsize.c, used internally by the cost estimation system
- Implements caching through RestrictInfo.eval_cost to avoid redundant calculations
- Does not account for short-circuit evaluation of AND/OR to maintain clause ordering independence
- Ignores set-returning function multiplication effects due to complexity and rarity
- Handles various PostgreSQL-specific node types including subplans, coercion, and JSON expressions
- Critical for accurate cost-based optimization across all plan types that evaluate expressions
- Returns false to prevent recursion for nodes with special handling (aggregates, subplans, placeholders)