# ExecEvalJsonIsPredicate

## Location
src/backend/executor/execExprInterp.c: 4179 - 4278

## Overview
Evaluates a JSON IS predicate to determine if a given JSON value matches a specific JSON type constraint (object, array, scalar, or any type).

## Definition
```c
void ExecEvalJsonIsPredicate(ExprState *state, ExprEvalStep *op)
```

## Detailed Description
This function implements the SQL/JSON IS JSON predicate evaluation, which tests whether a JSON value conforms to a specific JSON type. The function handles different input types (TEXT, JSON, JSONB) and can check for specific JSON types (object, array, scalar) or accept any valid JSON type. For TEXT input, it performs JSON validation including optional unique key checking. For JSONB input, it uses efficient binary format checks without redundant validation.

## Parameters / Member Variables
- `state`: ExprState containing the expression evaluation context
- `op`: ExprEvalStep containing the operation details and result storage
  - `op->d.is_json.pred`: JsonIsPredicate structure containing the predicate configuration
  - `op->resvalue`: Pointer to the input JSON datum and result storage
  - `op->resnull`: Pointer to null indicator flag

## Dependencies
- Functions called/Symbols referenced:
  - exprType
  - DatumGetTextP
  - json_get_first_token
  - json_validate
  - DatumGetJsonbP
  - BoolGetDatum
- Called from (representative examples):
  - ExecInterpExpr
  - FunctionReturningBool (via JIT compilation)

## Notes and Other Information
- Returns false immediately if the input is NULL
- For TEXT/JSON inputs, performs first-token analysis for type checking and optionally validates the entire JSON structure
- For JSONB inputs, uses efficient binary format macros (JB_ROOT_IS_OBJECT, JB_ROOT_IS_ARRAY, JB_ROOT_IS_SCALAR) for type checking
- Key uniqueness validation is skipped for JSONB since it's already guaranteed by the format
- Supports JS_TYPE_ANY for accepting any valid JSON type
- Used in SQL expressions like 'column IS JSON OBJECT' or 'column IS JSON WITH UNIQUE KEYS'