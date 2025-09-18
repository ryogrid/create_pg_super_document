# ExecEvalJsonExprPath

## Location
src/backend/executor/execExprInterp.c: 4279 - 4480

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