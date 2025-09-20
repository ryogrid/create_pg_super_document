# A_Expr_Kind

## Location
[src/include/nodes/parsenodes.h:327-328](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L327-L328)

## Overview
A_Expr_Kind is an enumeration that defines the various types of expressions that can be represented by A_Expr nodes in PostgreSQL's parser tree, including operators, comparisons, and special SQL constructs.

## Definition

```c
typedef struct A_Expr
{
	pg_node_attr(custom_read_write)

	NodeTag		type;
	A_Expr_Kind kind;			/* see above */
	List	   *name;			/* possibly-qualified name of operator */
	Node	   *lexpr;			/* left argument, or NULL if none */
	Node	   *rexpr;			/* right argument, or NULL if none */
	ParseLoc	location;		/* token location, or -1 if unknown */
} A_Expr;
```
## Detailed Description
A_Expr_Kind categorizes different types of expressions in PostgreSQL's abstract syntax tree. It provides a comprehensive classification system for infix, prefix, and postfix expressions that can appear in SQL queries. Each kind corresponds to specific SQL syntax patterns and has constraints on the operator names that can be used. The enum is used by the parser to differentiate between various expression types during query analysis and transformation.

## Parameters / Member Variables
- `AEXPR_OP`: Standard binary or unary operators (e.g., +, -, *, /)
- `AEXPR_OP_ANY`: Scalar comparison with ANY array operator (e.g., value = ANY(array))
- `AEXPR_OP_ALL`: Scalar comparison with ALL array operator (e.g., value > ALL(array))
- `AEXPR_DISTINCT`: IS DISTINCT FROM comparison, requires "=" as operator name
- `AEXPR_NOT_DISTINCT`: IS NOT DISTINCT FROM comparison, requires "=" as operator name
- `AEXPR_NULLIF`: NULLIF function expression, requires "=" as operator name
- `AEXPR_IN`: IN or NOT IN membership tests, requires "=" or "<>" as operator name
- `AEXPR_LIKE`: LIKE or NOT LIKE pattern matching, requires "~~" or "!~~" as operator name
- `AEXPR_ILIKE`: ILIKE or NOT ILIKE case-insensitive pattern matching, requires "~~*" or "!~~*"
- `AEXPR_SIMILAR`: SIMILAR TO or NOT SIMILAR TO regex matching, requires "~" or "!~"
- `AEXPR_BETWEEN`: BETWEEN range comparison
- `AEXPR_NOT_BETWEEN`: NOT BETWEEN range comparison
- `AEXPR_BETWEEN_SYM`: BETWEEN SYMMETRIC range comparison
- `AEXPR_NOT_BETWEEN_SYM`: NOT BETWEEN SYMMETRIC range comparison

## Dependencies
- Functions called/Symbols referenced:
  - None (enum definition)
- Called from (representative examples):
  - [A_Expr](A_Expr.md) (in kind field)
  - makeA_Expr
  - makeSimpleA_Expr

## Notes and Other Information
- Located in src/include/nodes/parsenodes.h:311-327
- Each expression kind has specific constraints on valid operator names
- Used extensively in SQL parsing to maintain semantic meaning of different expression types
- The BETWEEN variants handle symmetric vs. asymmetric range comparisons
- Pattern matching operators (LIKE, ILIKE, SIMILAR) use specialized operator symbols internally