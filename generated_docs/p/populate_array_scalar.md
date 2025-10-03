# populate_array_scalar

## Location
[src/backend/utils/adt/jsonfuncs.c:2751-2786](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L2751-L2786)

## Overview
A JSON parsing callback function that handles scalar values encountered during JSON array population, validating array dimensions and storing scalar tokens.

## Definition

```c
static JsonParseErrorType
populate_array_scalar(void *_state, char *token, JsonTokenType tokentype)
```
## Detailed Description
This function serves as a JSON semantic action callback specifically designed for handling scalar values (strings, numbers, booleans, null) during the JSON array population process. It performs critical validation to ensure that the JSON structure matches the expected array dimensions, and when appropriate, stores the scalar token for later processing. The function handles dimension validation by checking if scalars appear at the correct nesting level and reports errors when the structure doesn't match expectations.

## Parameters / Member Variables
- `*_state`: A void pointer cast to PopulateArrayState containing the parsing state and context
- `*token`: A character pointer to the scalar token string representation
- `tokentype`: The JsonTokenType indicating the specific type of the scalar (string, number, boolean, null)
## Dependencies
- Functions called/Symbols referenced:
  - [JsonTokenType](../J/JsonTokenType.md) (token type enumeration)
  - [PopulateArrayState](../P/PopulateArrayState.md) (state structure)
  - [PopulateArrayContext](../P/PopulateArrayContext.md) (context structure)  
  - [populate_array_assign_ndims](populate_array_assign_ndims.md) (dimension assignment function)
  - [populate_array_report_expected_array](populate_array_report_expected_array.md) (error reporting function)
  - JSON_SEM_ACTION_FAILED (error return constant)
  - SOFT_ERROR_OCCURRED (error checking macro)
  - JSON_SUCCESS (success return constant)
- Called from (representative examples):
  - JsObjectFree
  - [populate_array_json](populate_array_json.md)

## Notes and Other Information
- This is a static function within jsonfuncs.c, serving as an internal implementation detail
- The function performs dimension validation, ensuring scalars only appear at the expected array depth
- When encountering a scalar at the target dimension level, it stores the token in state->element_scalar for later processing
- Error handling includes both hard failures and soft error reporting through the error context system
- The function assumes element_type was already set by populate_array_element_start() when processing scalars at the target dimension
- Part of PostgreSQL's JSON semantic action callback infrastructure for array population operations

## Simplified Source

```c
static JsonParseErrorType
populate_array_scalar(void *_state, char *token, JsonTokenType tokentype)
{
    PopulateArrayState *state = (PopulateArrayState *) _state;
    PopulateArrayContext *ctx = state->ctx;
    int ndim = state->lex->lex_level;

    // Assign dimensions if not yet determined
    if (ctx->ndims <= 0) {
        if (!populate_array_assign_ndims(ctx, ndim))
            return JSON_SEM_ACTION_FAILED;
    }
    // Validate scalar appears at correct nesting level
    else if (ndim < ctx->ndims) {
        populate_array_report_expected_array(ctx, ndim);
        return JSON_SEM_ACTION_FAILED;
    }

    // Store scalar token if at target dimension
    if (ndim == ctx->ndims) {
        state->element_scalar = token;
        // element_type should match from populate_array_element_start()
        Assert(state->element_type == tokentype);
    }

    return JSON_SUCCESS;
}
```