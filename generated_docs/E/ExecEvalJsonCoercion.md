# ExecEvalJsonCoercion

## Location
[src/backend/executor/execExprInterp.c:4556-4607](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L4556-L4607)

## Overview
Coerces JSONB values produced by JSON expressions or behavior expressions to target data types, with special handling for boolean to integer conversion and domain constraint validation.

## Definition
```c
void ExecEvalJsonCoercion(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
```

## Detailed Description
This function performs type coercion for JSON expression results, converting JSONB values to target SQL types using json_populate_type(). It provides specialized handling for JSON_EXISTS operations that return boolean values, with optimized casting to integer types and domain constraint checking. The function supports soft error handling through ErrorSaveContext, allowing graceful error recovery in JSON expressions with ON ERROR clauses. For boolean results that need integer conversion, it bypasses the standard input function route which doesn't accept boolean literals.

## Parameters / Member Variables
- `state`: ExprState containing the expression evaluation context
- `op`: ExprEvalStep containing coercion configuration
  - `op->d.jsonexpr_coercion.escontext`: ErrorSaveContext for soft error handling
  - `op->d.jsonexpr_coercion.exists_coerce`: Flag indicating JSON_EXISTS boolean coercion
  - `op->d.jsonexpr_coercion.exists_cast_to_int`: Flag for boolean-to-integer optimization
  - `op->d.jsonexpr_coercion.exists_check_domain`: Flag for domain constraint checking
  - `op->d.jsonexpr_coercion.targettype`: Target data type OID
  - `op->d.jsonexpr_coercion.targettypmod`: Target type modifier
  - `op->d.jsonexpr_coercion.json_coercion_cache`: Cached coercion information
  - `op->d.jsonexpr_coercion.omit_quotes`: Flag for quote handling in coercion
  - `op->resvalue`: Pointer to result datum
  - `op->resnull`: Pointer to result null indicator
- `econtext`: ExprContext providing memory context and evaluation state

## Dependencies
- Functions called/Symbols referenced:
  - domain_check_safe
  - DirectFunctionCall1 (bool_int4, jsonb_in)
  - [DatumGetBool](../D/DatumGetBool.md)
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - [json_populate_type](../j/json_populate_type.md)
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md)
  - [FunctionReturningBool](../F/FunctionReturningBool.md) (via JIT compilation)

## Notes and Other Information
- Handles special case for boolean-to-integer coercion to avoid input function limitations
- Performs domain constraint validation when required for integer domains
- Uses json_populate_type for general JSONB-to-SQL type conversion
- Supports quote omission for string-like target types
- Works in conjunction with EEOP_JSONEXPR_COERCION_FINISH for error checking
- Part of the SQL/JSON coercion pipeline following path evaluation
- Critical for JSON_TABLE column type conversion and JSON_VALUE RETURNING clause handling
- [ErrorSaveContext](ErrorSaveContext.md) enables ON ERROR behavior implementation in parent JSON expressions
- Optimized path for common JSON_EXISTS to integer conversion in JSON_TABLE contexts