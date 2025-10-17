# get_object_field_end

## Location
[src/backend/utils/adt/jsonfuncs.c:1242-1292](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L1242-L1292)

## Overview
A JSON semantic action callback function that handles the completion of object field processing, extracting the field value and performing cleanup operations when a target field has been processed.

## Definition
static JsonParseErrorType get_object_field_end(void *state, char *fname, bool isnull)

## Detailed Description
The `get_object_field_end` function is a semantic action callback used during JSON parsing to handle the completion of object field processing. It performs the same path matching logic as `get_object_field_start` to determine if the current field is a target for extraction. When a target field is completed, the function extracts the field value by calculating the text range from the previously recorded start position to the current token terminator, then converts this text into a PostgreSQL text datum. The function also handles path state cleanup by resetting pathok flags for intermediate levels and manages special cases for null values when normalization is enabled.

## Parameters / Member Variables
- `state`: A void pointer to the GetState structure containing parsing context and state information
- `fname`: The name of the current object field being completed
- `isnull`: Boolean indicating if the field value is null, affects result handling when normalization is enabled

## Dependencies
- Functions called/Symbols referenced:
  - [GetState](../G/GetState.md)
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md)
  - JSON_SUCCESS
  - JsonParseErrorType
- Called from (representative examples):
  - [get_worker](get_worker.md)

## Notes and Other Information
- This function is static and internal to jsonfuncs.c
- It mirrors the path matching logic from get_object_field_start to identify target fields
- For intermediate path levels, it resets the pathok flag to clean up state after processing nested objects
- The function only extracts values when result_start is not NULL (indicating that get_object_field_start previously identified this as a target)
- Special handling for null values: when isnull is true and normalization is enabled, it sets tresult to NULL instead of extracting text
- The extracted text length is calculated using the lexer's prev_token_terminator position
- Includes cleanup by setting result_start to NULL after extraction
- Always returns JSON_SUCCESS regardless of whether extraction occurred

## Simplified Source

```c
static JsonParseErrorType
get_object_field_end(void *state, char *fname, bool isnull)
{
    GetState *_state = (GetState *) state;
    bool get_last = false;
    int lex_level = _state->lex->lex_level;

    // Check if this field matches our target path (same logic as field_start)
    if (lex_level <= _state->npath &&
        _state->pathok[lex_level - 1] &&
        _state->path_names != NULL &&
        _state->path_names[lex_level - 1] != NULL &&
        strcmp(fname, _state->path_names[lex_level - 1]) == 0) {

        if (lex_level < _state->npath) {
            // Intermediate level - clean up path state
            _state->pathok[lex_level] = false;
        } else {
            // Final level - extract this field value
            get_last = true;
        }
    }

    // Extract field value if this is our target and extraction was prepared
    if (get_last && _state->result_start != NULL) {
        // Handle null values in normalization mode
        if (isnull && _state->normalize_results) {
            _state->tresult = (text *) NULL;
        } else {
            // Extract text from recorded start position to current end
            const char *start = _state->result_start;
            int len = _state->lex->prev_token_terminator - start;
            _state->tresult = cstring_to_text_with_len(start, len);
        }

        // Clean up extraction state
        _state->result_start = NULL;
    }

    return JSON_SUCCESS;
}
```