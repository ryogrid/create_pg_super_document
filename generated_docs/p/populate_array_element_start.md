# populate_array_element_start

## Location
[src/backend/utils/adt/jsonfuncs.c:2690-2707](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L2690-L2707)

## Overview
JSON parse handler that processes the start of array elements during JSON array population, capturing element positioning and type information.

## Definition
```c
static JsonParseErrorType populate_array_element_start(void *_state, bool isnull)
```

## Detailed Description
This function serves as a JSON parsing event handler for the start of array elements during PostgreSQL array population from JSON. It captures critical information about each array element as parsing begins, including the token start position, token type, and initializes the scalar value storage. The function only processes elements that are at the appropriate dimensional level - either when dimensions are not yet determined or when the current nesting level matches the expected number of dimensions. This selective processing ensures that only actual array elements (not nested structure elements) are tracked for subsequent processing.

## Parameters / Member Variables
- `_state`: Void pointer to PopulateArrayState containing the parsing state and lexer information
- `isnull`: Boolean flag indicating whether the element value is null (currently unused in the implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [PopulateArrayState](../P/PopulateArrayState.md) (data type)
  - JsonParseErrorType (return type) 
  - JSON_SUCCESS (return constant)
- Called from (representative examples):
  - [populate_array_json](populate_array_json.md)
  - JsObjectFree

## Notes and Other Information
- Always returns JSON_SUCCESS as this handler does not perform validation that could fail
- Captures element start position (token_start) and type (token_type) for later processing
- Resets element_scalar to NULL to prepare for potential scalar value storage
- Part of the JSON semantic action handler system for array population
- Works in conjunction with populate_array_element_end to bracket element processing
- Critical for maintaining parsing state during element extraction from JSON arrays

## Simplified Source

```c
static JsonParseErrorType
populate_array_element_start(void *_state, bool isnull)
{
    PopulateArrayState *state = (PopulateArrayState *) _state;
    int ndim = state->lex->lex_level;

    // Capture element info if at correct dimensional level
    if (state->ctx->ndims <= 0 || ndim == state->ctx->ndims) {
        state->element_start = state->lex->token_start;
        state->element_type = state->lex->token_type;
        state->element_scalar = NULL;
    }

    return JSON_SUCCESS;
}
```