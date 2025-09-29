# transformExprRecurse

## Location
[src/backend/parser/parse_expr.c:138-391](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L138-L391)

## Overview
 is the core recursive function that performs the actual transformation of SQL expression trees, dispatching to specialized transformation functions based on node type.

## Definition

```c
struct and reconstruct column
			 * references, which seems expensively pointless.  So allow it.
			 */
		case T_CaseTestExpr:
		case T_Var:
			{
				result = (Node *) expr;
				break;
			}

		case T_JsonObjectConstructor:
			result = transformJsonObjectConstructor(pstate, (JsonObjectConstructor *) expr);
```
## Detailed Description
 serves as the central dispatcher for expression transformation in PostgreSQL's parser. It implements a comprehensive switch statement that handles over 30 different node types, from basic constants and column references to complex JSON expressions and subqueries. The function includes stack overflow protection and transforms raw grammar nodes into fully typed and semantically validated expression trees. Each case delegates to a specialized transformation function that handles the specific semantics of that expression type, ensuring proper type checking, operator resolution, and semantic validation throughout the expression tree.

## Parameters / Member Variables
- `expr`: ParseState structure containing current parsing context, including scope information, query structure, and parsing state
- `break`: The raw expression node from the parser that needs to be recursively transformed into a semantic expression tree

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md) (stack overflow protection)
  - nodeTag (node type identification)
  - transformColumnRef, transformParamRef, transformIndirection (basic expression types)
  - [transformAExprOp](transformAExprOp.md), transformAExprOpAny, transformAExprOpAll (operator expressions)  
  - [transformFuncCall](transformFuncCall.md), transformSubLink, transformCaseExpr (complex expressions)
  - [transformJsonObjectConstructor](transformJsonObjectConstructor.md), transformJsonArrayConstructor (JSON expressions)
  - [make_const](../m/make_const.md), type_is_rowtype (utility functions)
  - Various AEXPR_* and T_* constants for node type matching

- Called from (representative examples):
  - [transformExpr](transformExpr.md) (main entry point)
  - [transformExprRecurse](transformExprRecurse.md) (recursive self-calls for nested expressions)
  - [transformIndirection](transformIndirection.md), transformAExprOp, transformCaseExpr (specialized transformers)
  - [transformFuncCall](transformFuncCall.md), transformBoolExpr (for argument processing)

## Notes and Other Information
- Implements comprehensive stack overflow protection via check_stack_depth() to handle deeply nested expressions
- Handles over 30 different PostgreSQL node types in a single switch statement
- Includes special handling for DEFAULT expressions (which should be processed by callers, not passed through)
- Supports both traditional SQL expressions and modern JSON constructor/query expressions introduced in recent PostgreSQL versions
- The function is static, meaning it's only callable from within the parse_expr.c module
- Critical error handling for unrecognized node types to catch parser bugs during development
- Self-recursive design allows for proper transformation of arbitrarily nested expression structures

## Simplified Source

```c
static Node *
transformExprRecurse(ParseState *pstate, Node *expr)
{
    Node *result;

    // Handle null expression
    if (expr == NULL)
        return NULL;

    // Prevent stack overflow from deeply nested expressions
    check_stack_depth();

    // Dispatch based on expression node type
    switch (nodeTag(expr))
    {
        // Basic expressions
        case T_ColumnRef:
            result = transformColumnRef(pstate, (ColumnRef *) expr);
            break;
        case T_ParamRef:
            result = transformParamRef(pstate, (ParamRef *) expr);
            break;
        case T_A_Const:
            result = (Node *) make_const(pstate, (A_Const *) expr);
            break;

        // Complex expressions requiring sub-dispatching
        case T_A_Expr:
            result = transformAExpr(pstate, (A_Expr *) expr);
            break;
        case T_FuncCall:
            result = transformFuncCall(pstate, (FuncCall *) expr);
            break;
        case T_SubLink:
            result = transformSubLink(pstate, (SubLink *) expr);
            break;
        case T_CaseExpr:
            result = transformCaseExpr(pstate, (CaseExpr *) expr);
            break;

        // Array and type operations
        case T_A_ArrayExpr:
            result = transformArrayExpr(pstate, (A_ArrayExpr *) expr,
                                      InvalidOid, InvalidOid, -1);
            break;
        case T_TypeCast:
            result = transformTypeCast(pstate, (TypeCast *) expr);
            break;

        // JSON expressions (newer PostgreSQL features)
        case T_JsonObjectConstructor:
            result = transformJsonObjectConstructor(pstate, (JsonObjectConstructor *) expr);
            break;
        case T_JsonArrayConstructor:
            result = transformJsonArrayConstructor(pstate, (JsonArrayConstructor *) expr);
            break;

        // Special handling for already-transformed nodes
        case T_CaseTestExpr:
        case T_Var:
            result = expr;
            break;

        // Error cases
        case T_SetToDefault:
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                           errmsg("DEFAULT is not allowed in this context")));
            break;

        default:
            elog(ERROR, "unrecognized node type: %d", (int) nodeTag(expr));
            result = NULL;
            break;
    }

    return result;
}
```