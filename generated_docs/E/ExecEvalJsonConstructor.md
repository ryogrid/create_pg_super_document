# ExecEvalJsonConstructor

## Location
[src/backend/executor/execExprInterp.c:4101-4178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L4101-L4178)

## Overview
Evaluates JSON constructor expressions that build JSON arrays, objects, scalars, or parse JSON text into appropriate JSON/JSONB format.

## Definition
void ExecEvalJsonConstructor(ExprState *state, ExprEvalStep *op, ExprContext *econtext)

## Detailed Description
This function handles the evaluation of JSON constructor expressions, which are used to build JSON data structures from SQL values. It supports four main types of JSON construction operations:

- **JSCTOR_JSON_ARRAY**: Constructs JSON arrays from a list of values, with support for absent_on_null behavior
- **JSCTOR_JSON_OBJECT**: Constructs JSON objects from key-value pairs, supporting both absent_on_null and unique key constraints
- **JSCTOR_JSON_SCALAR**: Converts a single SQL value to its JSON representation using appropriate type conversion
- **JSCTOR_JSON_PARSE**: Parses a text string as JSON, validating and converting to the target JSON format

The function automatically determines whether to produce JSON (text) or JSONB (binary) output based on the constructor's returning format specification. For JSON output, validation is performed but the original text may be preserved, while JSONB output involves parsing and binary encoding.

## Parameters / Member Variables
- : Expression state context (unused in this function)
- : Expression evaluation step containing JSON constructor state and configuration
- : Expression evaluation context (unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  - [jsonb_build_array_worker](../j/jsonb_build_array_worker.md)
  - [json_build_array_worker](../j/json_build_array_worker.md)
  - [jsonb_build_object_worker](../j/jsonb_build_object_worker.md)
  - [json_build_object_worker](../j/json_build_object_worker.md)
  - [datum_to_jsonb](../d/datum_to_jsonb.md)
  - [datum_to_json](../d/datum_to_json.md)
  - [jsonb_from_text](../j/jsonb_from_text.md)
  - [json_validate](../j/json_validate.md)
  - DatumGetTextP
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md)
  - [FunctionReturningBool](../F/FunctionReturningBool.md) (via JIT compilation)

## Notes and Other Information
- The function determines output format (JSON vs JSONB) from the constructor's returning format specification
- NULL input handling varies by constructor type: arrays and objects filter nulls based on absent_on_null setting, while scalars and parse operations return NULL for NULL inputs
- JSON validation is performed for parsing operations to ensure well-formed JSON
- Object constructors support unique key constraints to prevent duplicate keys in the result
- Memory management relies on PostgreSQL's memory context system for temporary allocations
- Type conversion for scalars uses cached type information including output functions and JSON type categories
- Error handling includes validation for unknown constructor types

## Simplified Source

```c
void ExecEvalJsonConstructor(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
    JsonConstructorExprState *jcstate = op->d.json_constructor.jcstate;
    JsonConstructorExpr *ctor = jcstate->constructor;
    bool is_jsonb = (ctor->returning->format->format_type == JS_FORMAT_JSONB);
    Datum res;
    bool isnull = false;

    switch (ctor->type)
    {
        case JSCTOR_JSON_ARRAY:
            // Build JSON array from arguments
            res = (is_jsonb ? jsonb_build_array_worker : json_build_array_worker)
                    (jcstate->nargs, jcstate->arg_values, jcstate->arg_nulls,
                     jcstate->arg_types, ctor->absent_on_null);
            break;

        case JSCTOR_JSON_OBJECT:
            // Build JSON object from key-value pairs
            res = (is_jsonb ? jsonb_build_object_worker : json_build_object_worker)
                    (jcstate->nargs, jcstate->arg_values, jcstate->arg_nulls,
                     jcstate->arg_types, ctor->absent_on_null, ctor->unique);
            break;

        case JSCTOR_JSON_SCALAR:
            // Convert single value to JSON scalar
            if (jcstate->arg_nulls[0])
            {
                res = (Datum) 0;
                isnull = true;
            }
            else
            {
                Datum value = jcstate->arg_values[0];
                Oid outfuncid = jcstate->arg_type_cache[0].outfuncid;
                JsonTypeCategory category = (JsonTypeCategory) jcstate->arg_type_cache[0].category;

                res = is_jsonb ? datum_to_jsonb(value, category, outfuncid)
                               : datum_to_json(value, category, outfuncid);
            }
            break;

        case JSCTOR_JSON_PARSE:
            // Parse text string as JSON
            if (jcstate->arg_nulls[0])
            {
                res = (Datum) 0;
                isnull = true;
            }
            else
            {
                text *js = DatumGetTextP(jcstate->arg_values[0]);

                if (is_jsonb)
                    res = jsonb_from_text(js, true);
                else
                {
                    json_validate(js, true, true);
                    res = jcstate->arg_values[0];  // Return original text
                }
            }
            break;

        default:
            elog(ERROR, "invalid JsonConstructorExpr type %d", ctor->type);
    }

    *op->resvalue = res;
    *op->resnull = isnull;
}
```