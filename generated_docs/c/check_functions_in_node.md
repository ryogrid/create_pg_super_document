# check_functions_in_node

## Location
[src/backend/nodes/nodeFuncs.c:1900-2082](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L1900-L2082)

## Overview
Applies a checker function to each function OID contained in a given expression node to determine if any contained functions meet specific criteria.

## Definition

```c
struct-equivalent to OpExpr */
		case T_NullIfExpr:		/* struct-equivalent to OpExpr */
			{
				OpExpr	   *expr = (OpExpr *) node;

				/* Set opfuncid if it wasn't set already */
				set_opfuncid(expr);
				if (checker(expr->opfuncid, context))
					return true;
			}
			break;
```
## Detailed Description
This function examines a single expression node and applies a checker callback function to every SQL function OID contained within that node. It returns true if the checker function returns true for any of the contained functions, and false if the node contains no SQL-visible functions or if the checker returns false for all functions.

The function handles various expression node types including Aggref, WindowFunc, FuncExpr, OpExpr, DistinctExpr, NullIfExpr, ScalarArrayOpExpr, CoerceViaIO, and RowCompareExpr. For operator expressions, it ensures that opfuncid is set before checking. For CoerceViaIO, it checks both input and output functions. For RowCompareExpr, it checks all comparison operators.

Important: This function does NOT recurse into sub-expressions; it only examines the given node. Callers are responsible for controlling recursion through the expression tree.

## Parameters / Member Variables
- : The expression node to examine (must not be NULL)
- : Callback function that takes a function OID and context, returns bool
- : Arbitrary context data passed to the checker function

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag
  - set_opfuncid
  - set_sa_opfuncid
  - getTypeInputInfo
  - getTypeOutputInfo
  - get_opcode
  - exprType
  - lfirst_oid
- Called from (representative examples):
  - check_simple_rowfilter_expr_walker
  - contain_mutable_functions_walker
  - contain_volatile_functions_walker
  - contain_volatile_functions_not_nextval_walker
  - max_parallel_hazard_walker
  - contain_nonstrict_functions_walker
  - contain_leaked_vars_walker

## Notes and Other Information
- Does not recurse into sub-expressions - caller controls recursion
- Ignores MinMaxExpr, SQLValueFunction, XmlExpr, CoerceToDomain, and NextValueExpr nodes
- Sets operator function IDs if not already set before checking
- For CoerceViaIO, checks both source type's output function and target type's input function
- Essential for analyzing function properties like volatility, mutability, and parallel safety across expression trees