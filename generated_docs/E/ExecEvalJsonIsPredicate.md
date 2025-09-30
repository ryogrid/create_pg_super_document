# ExecEvalJsonIsPredicate

## Location
[src/backend/executor/execExprInterp.c:4179-4278](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L4179-L4278)

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
  - [exprType](../e/exprType.md)
  - DatumGetTextP
  - [json_get_first_token](../j/json_get_first_token.md)
  - [json_validate](../j/json_validate.md)
  - [DatumGetJsonbP](../D/DatumGetJsonbP.md)
  - [BoolGetDatum](../B/BoolGetDatum.md)
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md)
  - [FunctionReturningBool](../F/FunctionReturningBool.md) (via JIT compilation)

## Notes and Other Information
- Returns false immediately if the input is NULL
- For TEXT/JSON inputs, performs first-token analysis for type checking and optionally validates the entire JSON structure
- For JSONB inputs, uses efficient binary format macros (JB_ROOT_IS_OBJECT, JB_ROOT_IS_ARRAY, JB_ROOT_IS_SCALAR) for type checking
- Key uniqueness validation is skipped for JSONB since it's already guaranteed by the format
- Supports JS_TYPE_ANY for accepting any valid JSON type
- Used in SQL expressions like 'column IS JSON OBJECT' or 'column IS JSON WITH UNIQUE KEYS'

## Simplified Source

```c
void ExecEvalJsonIsPredicate(ExprState *state, ExprEvalStep *op)
{
    JsonIsPredicate *pred = op->d.is_json.pred;
    Datum js = *op->resvalue;
    bool res;

    // Return false for NULL input
    if (*op->resnull)
    {
        *op->resvalue = BoolGetDatum(false);
        return;
    }

    Oid exprtype = exprType(pred->expr);

    if (exprtype == TEXTOID || exprtype == JSONOID)
    {
        // Handle TEXT/JSON input
        text *json = DatumGetTextP(js);

        if (pred->item_type == JS_TYPE_ANY)
        {
            res = true;  // Accept any valid JSON
        }
        else
        {
            // Check specific JSON type based on first token
            switch (json_get_first_token(json, false))
            {
                case JSON_TOKEN_OBJECT_START:
                    res = (pred->item_type == JS_TYPE_OBJECT);
                    break;
                case JSON_TOKEN_ARRAY_START:
                    res = (pred->item_type == JS_TYPE_ARRAY);
                    break;
                case JSON_TOKEN_STRING:
                case JSON_TOKEN_NUMBER:
                case JSON_TOKEN_TRUE:
                case JSON_TOKEN_FALSE:
                case JSON_TOKEN_NULL:
                    res = (pred->item_type == JS_TYPE_SCALAR);
                    break;
                default:
                    res = false;
                    break;
            }
        }

        // Perform full validation if needed (for uniqueness or TEXT validation)
        if (res && (pred->unique_keys || exprtype == TEXTOID))
        {
            res = json_validate(json, pred->unique_keys, false);
        }
    }
    else if (exprtype == JSONBOID)
    {
        // Handle JSONB input with efficient binary checks
        if (pred->item_type == JS_TYPE_ANY)
        {
            res = true;
        }
        else
        {
            Jsonb *jb = DatumGetJsonbP(js);

            switch (pred->item_type)
            {
                case JS_TYPE_OBJECT:
                    res = JB_ROOT_IS_OBJECT(jb);
                    break;
                case JS_TYPE_ARRAY:
                    res = JB_ROOT_IS_ARRAY(jb) && !JB_ROOT_IS_SCALAR(jb);
                    break;
                case JS_TYPE_SCALAR:
                    res = JB_ROOT_IS_ARRAY(jb) && JB_ROOT_IS_SCALAR(jb);
                    break;
                default:
                    res = false;
                    break;
            }
        }
        // Key uniqueness is guaranteed for JSONB, no need to check
    }
    else
    {
        res = false;  // Non-JSON types are not valid JSON
    }

    *op->resvalue = BoolGetDatum(res);
}
```