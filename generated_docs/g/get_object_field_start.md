# get_object_field_start

## Location
[src/backend/utils/adt/jsonfuncs.c:1195-1241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L1195-L1241)

## Overview
A JSON semantic action callback function that handles the start of object field processing, determining whether a field matches the target path and should be extracted.

## Definition
static JsonParseErrorType get_object_field_start(void *state, char *fname, bool isnull)

## Detailed Description
The `get_object_field_start` function is a semantic action callback used during JSON parsing to handle the beginning of object field processing. It compares the current field name against the target path names to determine if this field should be extracted. The function manages path navigation by checking if the current lexical level matches the expected path depth and if the field name matches the corresponding entry in the path_names array. When a target field is found, it prepares for value extraction by clearing previous results and setting up the appropriate extraction mode based on whether result normalization is required.

## Parameters / Member Variables
- `state`: A void pointer to the GetState structure containing parsing context and state information
- `fname`: The name of the current object field being processed
- `isnull`: Boolean indicating if the field name is null (currently unused in the implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [GetState](../G/GetState.md)
  - JSON_TOKEN_STRING
  - JSON_SUCCESS
  - JsonParseErrorType
- Called from (representative examples):
  - [get_worker](get_worker.md)

## Notes and Other Information
- This function is static and internal to jsonfuncs.c
- It performs path matching by comparing field names at the current lexical level with the target path
- The function handles both intermediate path navigation (setting pathok for deeper levels) and final target identification
- When a target field is found, it clears any previous results to handle object overrides
- The normalization behavior differs based on token type: for string tokens in normalize mode, it delegates to get_scalar; otherwise it records the starting position
- The function supports nested object navigation by maintaining path state across multiple lexical levels
- Always returns JSON_SUCCESS regardless of whether a match is found

## Simplified Source

```c
static JsonParseErrorType
get_object_field_start(void *state, char *fname, bool isnull)
{
    GetState *_state = (GetState *) state;
    bool get_next = false;
    int lex_level = _state->lex->lex_level;

    // Check if this field matches our target path
    if (lex_level <= _state->npath &&
        _state->pathok[lex_level - 1] &&
        _state->path_names != NULL &&
        _state->path_names[lex_level - 1] != NULL &&
        strcmp(fname, _state->path_names[lex_level - 1]) == 0) {

        if (lex_level < _state->npath) {
            // Intermediate path level - mark as valid for deeper navigation
            _state->pathok[lex_level] = true;
        } else {
            // Final path level - this is our target field
            get_next = true;
        }
    }

    if (get_next) {
        // Clear previous results (handles object field overrides)
        _state->tresult = NULL;
        _state->result_start = NULL;

        // Set up extraction mode based on normalization requirements
        if (_state->normalize_results &&
            _state->lex->token_type == JSON_TOKEN_STRING) {
            // For text variants, delegate to get_scalar for normalization
            _state->next_scalar = true;
        } else {
            // For JSON variants, record starting position for later extraction
            _state->result_start = _state->lex->token_start;
        }
    }

    return JSON_SUCCESS;
}
```