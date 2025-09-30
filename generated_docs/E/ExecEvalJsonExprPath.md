# ExecEvalJsonExprPath

## Location
[src/backend/executor/execExprInterp.c:4279-4480](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L4279-L4480)

## Overview
Evaluates a JSONPath expression against a JSON document to extract, query, or check existence of values, handling different SQL/JSON operations with comprehensive error and empty result management.

## Definition
```c
int ExecEvalJsonExprPath(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
```

## Detailed Description
This function is the core executor for SQL/JSON path expressions, supporting JSON_EXISTS, JSON_QUERY, and JSON_VALUE operations. It evaluates a JSONPath against a JSON document and handles the complete lifecycle including path evaluation, result coercion, error handling (ON ERROR), and empty result handling (ON EMPTY). The function determines the next execution step based on the operation result and configured behaviors, supporting both error-throwing and error-suppressing modes.

## Parameters / Member Variables
- `state`: ExprState containing the expression evaluation context
- `op`: ExprEvalStep containing the operation details and JsonExprState
  - `op->d.jsonexpr.jsestate`: JsonExprState with operation configuration and jump targets
  - `op->resvalue`: Pointer to result datum storage
  - `op->resnull`: Pointer to result null indicator
- `econtext`: ExprContext for expression evaluation context

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetJsonPathP](../D/DatumGetJsonPathP.md)
  - [JsonPathExists](../J/JsonPathExists.md) (for JSON_EXISTS_OP)
  - [JsonPathQuery](../J/JsonPathQuery.md) (for JSON_QUERY_OP)
  - [JsonPathValue](../J/JsonPathValue.md) (for JSON_VALUE_OP)
  - [ExecGetJsonValueItemString](ExecGetJsonValueItemString.md)
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md)
  - DirectFunctionCall1 (textin, jsonb_out)
  - FunctionCallInvoke
  - SOFT_ERROR_OCCURRED
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md)
  - [FunctionReturningBool](../F/FunctionReturningBool.md) (via JIT compilation)

## Notes and Other Information
- Returns integer indicating next step to execute (jump_error, jump_empty, jump_eval_coercion, or jump_end)
- Resets error and empty state for each evaluation cycle
- Supports different RETURNING types (JSON, JSONB, text) with appropriate coercion
- Uses ErrorSaveContext for capturing and managing coercion errors
- JSON_EXISTS returns boolean existence result
- JSON_QUERY returns JSON/JSONB wrapped results based on wrapper setting
- JSON_VALUE extracts scalar values with type coercion support
- Handles ON ERROR and ON EMPTY behaviors including NULL return, default values, or error throwing
- Key uniqueness validation and formatting are handled in preceding expression steps
- Used extensively in SQL/JSON queries like JSON_VALUE(doc, '$.path' RETURNING int DEFAULT 0 ON EMPTY)

## Simplified Source

```c
int ExecEvalJsonExprPath(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
    JsonExprState *jsestate = op->d.jsonexpr.jsestate;
    JsonExpr *jsexpr = jsestate->jsexpr;
    Datum item = jsestate->formatted_expr.value;
    JsonPath *path = DatumGetJsonPathP(jsestate->pathspec.value);
    bool throw_error = (jsexpr->on_error->btype == JSON_BEHAVIOR_ERROR);
    bool error = false, empty = false;

    // Reset error/empty state for this evaluation
    reset_json_expr_state(jsestate);

    // Execute the appropriate JSON operation
    switch (jsexpr->op)
    {
        case JSON_EXISTS_OP:
        {
            bool exists = JsonPathExists(item, path, !throw_error ? &error : NULL, jsestate->args);
            if (!error)
            {
                *op->resnull = false;
                *op->resvalue = BoolGetDatum(exists);
            }
            break;
        }

        case JSON_QUERY_OP:
            *op->resvalue = JsonPathQuery(item, path, jsexpr->wrapper, &empty,
                                        !throw_error ? &error : NULL, jsestate->args,
                                        jsexpr->column_name);
            *op->resnull = (DatumGetPointer(*op->resvalue) == NULL);
            break;

        case JSON_VALUE_OP:
        {
            JsonbValue *jbv = JsonPathValue(item, path, &empty, !throw_error ? &error : NULL,
                                          jsestate->args, jsexpr->column_name);

            if (jbv == NULL)
            {
                *op->resvalue = (Datum) 0;
                *op->resnull = true;
            }
            else if (!error && !empty)
            {
                // Convert result based on returning type
                handle_json_value_result(jbv, jsexpr, op);
            }
            break;
        }

        default:
            elog(ERROR, "unrecognized SQL/JSON expression op %d", (int) jsexpr->op);
    }

    // Perform IO coercion if needed
    if (!*op->resnull && jsexpr->use_io_coercion)
    {
        perform_io_coercion(jsestate, op, &error);
    }

    // Handle ON EMPTY behavior
    if (empty)
    {
        return handle_empty_result(jsestate, jsexpr, op);
    }

    // Handle ON ERROR behavior
    if (error)
    {
        return handle_error_result(jsestate, jsexpr, op);
    }

    // Return next step to execute
    return jsestate->jump_eval_coercion >= 0 ? jsestate->jump_eval_coercion : jsestate->jump_end;
}
```