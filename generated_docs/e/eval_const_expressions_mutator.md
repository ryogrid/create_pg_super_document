# eval_const_expressions_mutator

## Location
[src/backend/optimizer/util/clauses.c:2440-3735](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L2440-L3735)

## Overview
This static function implements the core recursive logic for constant expression evaluation and simplification, serving as the workhorse behind both eval_const_expressions and estimate_expression_value functions.

## Definition

```c
struct
													 * equivalence */

					/*
					 * Code for op/func reduction is pretty bulky, so split it
					 * out as a separate function.
					 */
					simple = simplify_function(expr->opfuncid,
											   expr->opresulttype, -1,
											   expr->opcollid,
											   expr->inputcollid,
											   &args,
											   false,
											   false,
											   false,
											   context);
```
## Detailed Description
The  function is the recursive engine that performs comprehensive constant folding, expression simplification, and optimization across all PostgreSQL expression node types. It implements a tree-walking mutator pattern that examines each node type and applies appropriate simplifications while preserving expression semantics.

Key capabilities include:
- **Parameter substitution**: Replaces Param nodes with constant values when bound parameters are available and context permits
- **Function/operator evaluation**: Simplifies FuncExpr and OpExpr nodes by calling simplify_function for immutable operations
- **Boolean logic optimization**: Reduces BoolExpr (AND/OR/NOT) expressions using logical identities  
- **Type coercion simplification**: Optimizes RelabelType, CoerceViaIO, and other coercion expressions
- **CASE expression optimization**: Eliminates unreachable branches and constant conditions
- **Array/window function handling**: Processes complex expressions while expanding function arguments
- **Special expression types**: Handles DistinctExpr, NullTest, JsonValueExpr, and other specialized nodes

The function respects context settings to determine whether unsafe optimizations (estimate mode) are permitted and maintains proper recursion through expression trees.

## Parameters / Member Variables
- : Node pointer to the current expression node being processed and potentially optimized
- : eval_const_expressions_context structure containing bound parameters, estimation mode flags, active function list, and CASE context information

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth, nodeTag, copyObject
  - Parameter handling: ParamListInfo, ParamExternData, get_typlenbyval, datumCopy, makeConst
  - Function processing: expand_function_arguments, simplify_function, set_opfuncid
  - Boolean logic: simplify_or_arguments, simplify_and_arguments, negate_clause, make_andclause, make_orclause
  - Expression evaluation: ece_evaluate_expr, ece_function_is_safe, ece_all_arguments_const
  - Type handling: applyRelabelType, getTypeOutputInfo, getTypeInputInfo
  - Many specialized helper functions for different expression types
- Called from (representative examples):
  - [eval_const_expressions](eval_const_expressions.md) (clauses.c:2266)
  - [estimate_expression_value](estimate_expression_value.md) (clauses.c:2405)
  - (recursively calls itself throughout expression trees)

## Notes and Other Information
- Uses stack depth checking to prevent overflow during deep recursion
- Implements the expression tree mutator pattern, returning modified copies of nodes
- Central to PostgreSQL's query optimization pipeline, enabling significant performance improvements through constant folding
- Handles over 20 different PostgreSQL expression node types with specialized logic for each
- The function's behavior is controlled by context flags, allowing different optimization strategies for planning vs. execution
- Critical for eliminating redundant computation and enabling further optimizations in the query planner