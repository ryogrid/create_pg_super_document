# get_array_start

## Location
[src/backend/utils/adt/jsonfuncs.c:1293-1332](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L1293-L1332)

## Overview
A static callback function used during JSON parsing to handle the beginning of JSON arrays, managing array indexing and element counting for path-based JSON extraction operations.

## Definition
```c
static JsonParseErrorType get_array_start(void *state)
```

## Detailed Description
The `get_array_start` function is a JSON parser callback that processes the start of JSON arrays during parsing operations. It handles array initialization, manages current array indexes, and processes negative array subscripts by converting them to positive indices. The function is part of PostgreSQL's JSON path extraction mechanism, where it ensures proper navigation through nested JSON structures when extracting specific elements or subarrays.

The function performs several key operations:
- Initializes element counting for arrays at the current lexical level
- Handles negative array subscripts by counting total elements and converting to positive indices
- Sets up result capture for cases where the entire array should be matched
- Manages the parsing state for nested array structures

## Parameters / Member Variables
- `state`: A void pointer that is cast to `GetState *`, containing the parsing state including lexical analyzer, path information, current array indices, and result tracking

## Dependencies
- Functions called/Symbols referenced:
  - [GetState](../G/GetState.md) (struct type for casting state parameter)
  - JsonParseErrorType (return type and error handling)
  - [json_count_array_elements](../j/json_count_array_elements.md) (counts elements in array for negative index conversion)
  - [json_errsave_error](../j/json_errsave_error.md) (error reporting function)
  - JSON_SUCCESS (success return constant)
- Called from (representative examples):
  - [get_worker](get_worker.md) (JSON extraction worker function)
  - JsObjectFree (JSON object processing)

## Notes and Other Information
- This function is specifically designed for path-based JSON extraction operations
- Negative array indices (e.g., -1 for last element) are supported and automatically converted to positive indices
- The function handles special case logic for matching entire arrays at the outermost level
- INT_MIN is used as a reserved value to represent invalid subscripts
- The function integrates with PostgreSQL's JSON lexical analyzer system for parsing

## Simplified Source

```c
static JsonParseErrorType get_array_start(void *state) {
    GetState *_state = (GetState *) state;
    int lex_level = _state->lex->lex_level;

    if (lex_level < _state->npath) {
        // Initialize array element counting
        _state->array_cur_index[lex_level] = -1;

        // Handle negative array subscripts (convert to positive indices)
        if (_state->path_indexes[lex_level] < 0 &&
            _state->path_indexes[lex_level] != INT_MIN) {

            // Count total array elements
            int nelements;
            JsonParseErrorType error = json_count_array_elements(_state->lex, &nelements);
            if (error != JSON_SUCCESS)
                json_errsave_error(error, _state->lex, NULL);

            // Convert negative index to positive (e.g., -1 becomes last element)
            if (-_state->path_indexes[lex_level] <= nelements)
                _state->path_indexes[lex_level] += nelements;
        }
    }
    else if (lex_level == 0 && _state->npath == 0) {
        // Special case: match entire array at outermost level
        _state->result_start = _state->lex->token_start;
    }

    return JSON_SUCCESS;
}
```