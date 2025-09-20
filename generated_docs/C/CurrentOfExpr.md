# CurrentOfExpr

## Location
[src/include/nodes/primnodes.h:2094-2100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L2094-L2100)

## Overview
A node representing the "[WHERE] CURRENT OF cursor_name" clause, used to identify the current row of a cursor for UPDATE or DELETE operations.

## Definition

```c
typedef struct CurrentOfExpr
{
	Expr		xpr;
	Index		cvarno;			/* RT index of target relation */
	char	   *cursor_name;	/* name of referenced cursor, or NULL */
	int			cursor_param;	/* refcursor parameter number, or 0 */
} CurrentOfExpr;
```
## Detailed Description
CurrentOfExpr represents the CURRENT OF clause in SQL UPDATE and DELETE statements, which allows operations to target the current row of a cursor. This expression is similar to a Var node in that it carries the range table index of the target relation, enabling proper placement during query planning. The expression always has "levelsup" of zero due to syntactic constraints on where it can appear, and cvarno is always a true range table index (never special values like INNER_VAR).

The referenced cursor can be specified in two ways: as a hardwired string name (cursor_name) or as a reference to a run-time parameter of type REFCURSOR (cursor_param). The latter approach is primarily used for convenience in plpgsql stored procedures where cursor names might be determined dynamically.

## Parameters / Member Variables
- : Base expression node structure (inherited from Expr)
- : Range table index of the target relation being constrained
- : Name of the referenced cursor as a string, or NULL if using parameter reference
- : Parameter number for a REFCURSOR parameter, or 0 if using cursor_name

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)
- Called from (representative examples):
  - [execCurrentOf](../e/execCurrentOf.md) (src/backend/executor/execCurrent.c:44)
  - [TidExprListCreate](../T/TidExprListCreate.md) (src/backend/executor/nodeTidscan.c:109)
  - [IsCurrentOfClause](../I/IsCurrentOfClause.md) (src/backend/optimizer/path/tidpath.c:213)
  - transformCurrentOfExpr (src/backend/parser/parse_expr.c:2568)

## Notes and Other Information
- Used exclusively in UPDATE and DELETE statements with WHERE CURRENT OF clauses
- The cursor must be scrollable or positioned at a valid row for the operation to succeed
- During execution, the current cursor position is used to determine the target tuple ID (TID)
- The levelsup is always zero because CURRENT OF can only appear in the immediate query level
- Supports both static cursor names and dynamic cursor references through parameters
- The expression is processed during TID scan execution to locate the specific row to modify
- Part of PostgreSQL's cursor-based row positioning mechanism for precise row operations