# EvaluateParams

## Location
[src/backend/commands/prepare.c:278-368](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/prepare.c#L278-L368)

## Overview
Evaluates a list of parameter expressions for a prepared statement, performing type checking, coercion, and expression evaluation to produce a ParamListInfo structure for query execution.

## Definition

```c
static ParamListInfo
EvaluateParams(ParseState *pstate, PreparedStatement *pstmt, List *params,
			   EState *estate)
```
## Detailed Description
EvaluateParams processes parameter expressions provided to an EXECUTE statement by validating parameter count, performing parse analysis and type transformation on each parameter expression, coercing parameter values to expected types, and evaluating the expressions to produce concrete parameter values. The function ensures type safety by checking that provided parameters can be coerced to the types expected by the prepared statement, and produces a ParamListInfo structure that can be used during query execution.

## Parameters / Member Variables
- : Parse state containing parsing context and source text information
- : PreparedStatement containing parameter type information and metadata
- : List of parameter expressions (raw parser output) provided in the EXECUTE statement
- : Executor state used for expression evaluation context

## Dependencies
- Functions called/Symbols referenced:
  - copyObject (creates copy of parameter expressions)
  - [transformExpr](../t/transformExpr.md) (performs parse analysis on expressions)
  - [exprType](../e/exprType.md) (determines expression result type)
  - [coerce_to_target_type](../c/coerce_to_target_type.md) (performs type coercion)
  - [assign_expr_collations](../a/assign_expr_collations.md) (handles collation assignment)
  - [ExecPrepareExprList](ExecPrepareExprList.md) (prepares expressions for execution)
  - [makeParamList](../m/makeParamList.md) (creates ParamListInfo structure)
  - [ExecEvalExprSwitchContext](ExecEvalExprSwitchContext.md) (evaluates expressions)
  - GetPerTupleExprContext (gets evaluation context)
- Called from (representative examples):
  - [ExecuteQuery](ExecuteQuery.md) (parameter evaluation for EXECUTE statements)
  - [ExplainExecuteQuery](ExplainExecuteQuery.md) (parameter evaluation for EXPLAIN EXECUTE)

## Notes and Other Information
- Validates that the number of provided parameters matches the expected count from the prepared statement
- Returns NULL when no parameters are required, optimizing the common case
- Performs type coercion using assignment semantics, allowing implicit casts where appropriate
- Copies parameter expressions before transformation to avoid modifying parser input
- Evaluates parameters in per-tuple expression context for proper memory management
- Marks all parameters with PARAM_FLAG_CONST since they represent constant values
- Provides detailed error messages for parameter count mismatches and type coercion failures
- Handles collation assignment for string-type parameters to ensure correct comparison semantics

## Simplified Source

```c
static ParamListInfo EvaluateParams(ParseState *pstate, PreparedStatement *pstmt,
                                   List *params, EState *estate) {
    Oid *param_types = pstmt->plansource->param_types;
    int num_params = pstmt->plansource->num_params;
    int nparams = list_length(params);
    ParamListInfo paramLI;
    List *exprstates;
    ListCell *l;
    int i;

    // Validate parameter count
    if (nparams != num_params) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                       errmsg("wrong number of parameters for prepared statement \"%s\"",
                              pstmt->stmt_name),
                       errdetail("Expected %d parameters but got %d.",
                                num_params, nparams)));
    }

    // Handle no parameters case
    if (num_params == 0) {
        return NULL;
    }

    // Copy parameter expressions (parser doesn't like modifications)
    params = copyObject(params);

    // Transform and type-check each parameter
    i = 0;
    foreach(l, params) {
        Node *expr = lfirst(l);
        Oid expected_type_id = param_types[i];
        Oid given_type_id;

        // Parse analysis on the expression
        expr = transformExpr(pstate, expr, EXPR_KIND_EXECUTE_PARAMETER);
        given_type_id = exprType(expr);

        // Coerce to expected type
        expr = coerce_to_target_type(pstate, expr, given_type_id,
                                   expected_type_id, -1,
                                   COERCION_ASSIGNMENT,
                                   COERCE_IMPLICIT_CAST, -1);

        if (expr == NULL) {
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                           errmsg("parameter $%d of type %s cannot be coerced to the expected type %s",
                                  i + 1, format_type_be(given_type_id),
                                  format_type_be(expected_type_id)),
                           errhint("You will need to rewrite or cast the expression.")));
        }

        // Handle collations
        assign_expr_collations(pstate, expr);
        lfirst(l) = expr;
        i++;
    }

    // Prepare expressions for execution
    exprstates = ExecPrepareExprList(params, estate);
    paramLI = makeParamList(num_params);

    // Evaluate each parameter expression
    i = 0;
    foreach(l, exprstates) {
        ExprState *n = (ExprState *) lfirst(l);
        ParamExternData *prm = &paramLI->params[i];

        prm->ptype = param_types[i];
        prm->pflags = PARAM_FLAG_CONST;
        prm->value = ExecEvalExprSwitchContext(n, GetPerTupleExprContext(estate),
                                             &prm->isnull);
        i++;
    }

    return paramLI;
}
```