# NamedArgExpr

## Location
src/include/nodes/primnodes.h: 787 - 798

## Overview
NamedArgExpr represents a named argument of a function call in PostgreSQL's query tree, supporting both positional and named function argument notation.

## Definition

```c
typedef struct NamedArgExpr
{
	Expr		xpr;
	/* the argument expression */
	Expr	   *arg;
	/* the name */
	char	   *name pg_node_attr(query_jumble_ignore);
	/* argument's number in positional notation */
	int			argnumber;
	/* argument name location, or -1 if unknown */
	ParseLoc	location;
} NamedArgExpr;
```
## Detailed Description
NamedArgExpr is a node type that represents named arguments in function calls, appearing only in the arguments list of FuncCall or FuncExpr nodes. PostgreSQL supports three function call notations:
- Pure positional notation (no named arguments)
- Named notation (all arguments are named) 
- Mixed notation (unnamed arguments followed by named ones)

During parse analysis, the argnumber field is set to the positional index of the argument, but the argument list structure is preserved. The planner later converts all argument lists to pure positional notation during expression preprocessing, which means execution never encounters NamedArgExpr nodes - they are resolved away before runtime.

## Parameters / Member Variables
- : Base Expr node structure containing common expression fields
- : Pointer to the actual argument expression being passed to the function
- : String containing the parameter name, ignored during query jumbling for plan caching
- : Integer representing the argument's position in the function's parameter list (0-based)
- : Parse location of the argument name in the original query text, or -1 if location is unknown

## Dependencies
- Functions called/Symbols referenced:
  - ParseLoc
- Called from (representative examples):
  - exprType (for type analysis)
  - exprLocation (for error reporting)
  - transformExprRecurse (during parse analysis)
  - ParseFuncOrColumn (during function call parsing)
  - func_get_detail (for function resolution)
  - make_fn_arguments (for argument processing)
  - get_rule_expr (for query deparsing)

## Notes and Other Information
- Only appears in parse tree nodes (FuncCall, FuncExpr) and is eliminated before execution
- The query jumbler ignores the name field to ensure that functionally equivalent queries with different parameter naming styles generate the same plan cache key
- Critical for supporting SQL standard named parameter syntax in function calls
- Location tracking enables accurate error reporting when named arguments are used incorrectly