# get_object_start

## Location
[src/backend/utils/adt/jsonfuncs.c:1158-1176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L1158-L1176)

## Overview
A JSON semantic action callback function that handles the start of JSON object parsing, specifically managing the case where the entire root object should be matched.

## Definition
static JsonParseErrorType get_object_start(void *state)

## Detailed Description
The `get_object_start` function is a semantic action callback used during JSON parsing to handle the beginning of JSON objects. It performs a special case check for when the entire outermost object should be matched (when lex_level is 0 and npath is 0). In this scenario, it records the starting position of the object token for later extraction. The function is part of the JSON parsing infrastructure and works in conjunction with other semantic action functions to navigate and extract data from JSON structures.

## Parameters / Member Variables
- `state`: A void pointer to the GetState structure containing parsing context and state information

## Dependencies
- Functions called/Symbols referenced:
  - [GetState](../G/GetState.md)
  - JSON_SUCCESS
  - JsonParseErrorType
- Called from (representative examples):
  - [get_worker](get_worker.md)

## Notes and Other Information
- This function is static and internal to jsonfuncs.c
- It only performs meaningful work at the outermost lexical level (lex_level == 0) when no path is specified (npath == 0)
- The special case handling ensures that when extracting the entire root object, the starting position is properly recorded
- At nested levels, the match would have been initiated by outer field or array element callbacks
- Always returns JSON_SUCCESS to indicate successful processing

## Simplified Source

```c
static JsonParseErrorType
get_object_start(void *state)
{
    GetState *_state = (GetState *) state;
    int lex_level = _state->lex->lex_level;

    // Special case: match entire root object when no path specified
    if (lex_level == 0 && _state->npath == 0) {
        // Record start position for entire object extraction
        _state->result_start = _state->lex->token_start;
    }

    return JSON_SUCCESS;
}
```