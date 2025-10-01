# populate_array_element_end

## Location
[src/backend/utils/adt/jsonfuncs.c:2708-2750](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L2708-L2750)

## Overview
A JSON parsing callback function that handles the end of array elements during JSON array population, specifically for the populate_array_json() function.

## Definition

```c
static JsonParseErrorType
populate_array_element_end(void *_state, bool isnull)
```
## Detailed Description
This function serves as a JSON semantic action callback that is invoked when the JSON parser reaches the end of an array element. It is specifically designed to work with the populate_array_json() functionality. The function constructs a JsValue structure representing the completed array element and delegates the actual element processing to populate_array_element(). It handles both null values and non-null values, managing different representations based on whether the element is a scalar value or a complex JSON structure.

## Parameters / Member Variables
- : A void pointer that is cast to PopulateArrayState, containing the parsing state and context information
- : A boolean flag indicating whether the current array element is null

## Dependencies
- Functions called/Symbols referenced:
  - [PopulateArrayState](../P/PopulateArrayState.md) (state structure)
  - [PopulateArrayContext](../P/PopulateArrayContext.md) (context structure)
  - [JsValue](../J/JsValue.md) (value representation structure)
  - JSON_TOKEN_NULL (token constant)
  - [populate_array_element](populate_array_element.md) (element processing function)
  - JSON_SEM_ACTION_FAILED (error return value)
  - JSON_SUCCESS (success return value)
  - JsonParseErrorType (return type)
- Called from (representative examples):
  - JsObjectFree
  - [populate_array_json](populate_array_json.md)

## Notes and Other Information
- This is a static function within jsonfuncs.c, indicating it's an internal implementation detail
- The function operates at the lexical level depth corresponding to the target array dimension
- It handles both scalar elements (using element_scalar) and complex elements (using element_start and length calculations)
- The function is part of the JSON semantic action callback system used by PostgreSQL's JSON parser
- Error handling is delegated to the populate_array_element function, with this function only reporting failures

## Simplified Source

```c
static JsonParseErrorType
populate_array_element_end(void *_state, bool isnull)
{
    PopulateArrayState *state = (PopulateArrayState *) _state;
    PopulateArrayContext *ctx = state->ctx;
    int ndim = state->lex->lex_level;

    Assert(ctx->ndims > 0);

    // Process elements at target dimension level
    if (ndim == ctx->ndims)
    {
        JsValue jsv;

        // Set up JSON value structure
        jsv.is_json = true;
        jsv.val.json.type = state->element_type;

        if (isnull)
        {
            Assert(jsv.val.json.type == JSON_TOKEN_NULL);
            jsv.val.json.str = NULL;
            jsv.val.json.len = 0;
        }
        else if (state->element_scalar)
        {
            jsv.val.json.str = state->element_scalar;
            jsv.val.json.len = -1; // null-terminated
        }
        else
        {
            jsv.val.json.str = state->element_start;
            jsv.val.json.len = (state->lex->prev_token_terminator -
                               state->element_start) * sizeof(char);
        }

        // Process the element
        if (!populate_array_element(ctx, ndim, &jsv))
            return JSON_SEM_ACTION_FAILED;
    }

    return JSON_SUCCESS;
}
```