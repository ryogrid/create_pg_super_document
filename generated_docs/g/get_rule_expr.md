# get_rule_expr

## Location
[src/backend/utils/adt/ruleutils.c:8956-10324](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L8956-L10324)

## Overview
The main recursive function for converting PostgreSQL parse tree nodes back into SQL expressions during rule deparsing.

## Definition

```c
struct as an ANY/ALL
				 * SubLink.  To prevent misparsing the output that way, insert
				 * a dummy coercion (which will be stripped by parse analysis,
				 * so no inefficiency is added in dump and reload).  This is
				 * indeed most likely what the user wrote to get the construct
				 * accepted in the first place.
				 */
				if (IsA(arg2, SubLink) &&
					((SubLink *) arg2)->subLinkType == EXPR_SUBLINK)
					appendStringInfo(buf, "::%s",
									 format_type_with_typemod(exprType(arg2),
															  exprTypmod(arg2)));
```
## Detailed Description
 is the central function in PostgreSQL's rule deparsing system that recursively converts various types of expression nodes from the internal parse tree back into their SQL string representation. The function handles over 40 different node types including variables, constants, operators, functions, subqueries, and complex expressions like CASE statements and XML operations.

The function ensures that each level emits an indivisible term (parenthesized if necessary) to guarantee the output can be reparsed into the same expression tree. It performs extensive switch-case handling based on nodeTag() to process each expression type appropriately.

Key design principles:
- Maintains expression tree fidelity through proper parenthesization
- Handles implicit vs. explicit type coercions based on showimplicit parameter
- Manages formatting and indentation through the deparse_context
- Provides special handling for complex cases like subplans and XML expressions

## Parameters / Member Variables
- : The parse tree node to convert to SQL text (can be NULL)
- : Deparse context containing output buffer, formatting options, and namespace information
- : Boolean flag controlling whether implicit casts are displayed in the output

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS
  - [check_stack_depth](../c/check_stack_depth.md)
  - nodeTag
  - [get_variable](get_variable.md), get_const_expr, get_parameter
  - [get_agg_expr](get_agg_expr.md), get_windowfunc_expr, get_func_expr
  - [get_oper_expr](get_oper_expr.md), get_sublink_expr
  - [appendStringInfo](../a/appendStringInfo.md), appendStringInfoString, appendStringInfoChar
  - Many specialized helper functions for specific node types

- Called from (representative examples):
  - [deparse_expression_pretty](../d/deparse_expression_pretty.md)
  - [get_rule_expr_toplevel](get_rule_expr_toplevel.md)
  - [get_rule_expr_funccall](get_rule_expr_funccall.md)
  - Various other rule deparsing functions
  - Recursively calls itself for nested expressions

## Notes and Other Information
- Core function of PostgreSQL's rule system for converting internal representations to SQL
- Handles guard checks against excessively long or deeply-nested queries
- Special handling for Lists where component items are emitted comma-separated
- Contains extensive logic for proper formatting and parenthesization
- Critical for EXPLAIN output, view definitions, and rule reconstruction
- Must maintain exact semantic equivalence between input and output expressions