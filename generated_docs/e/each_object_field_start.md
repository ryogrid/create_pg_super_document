# each_object_field_start

## Location
[src/backend/utils/adt/jsonfuncs.c:2096-2117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L2096-L2117)

## Overview
The each_object_field_start function is a JSON parsing callback that handles the start of object field processing during JSON expansion operations.

## Definition
```c
static JsonParseErrorType each_object_field_start(void *state, char *fname, bool isnull)
```

## Detailed Description
This function serves as a callback handler in PostgreSQL's JSON parsing infrastructure, specifically designed to handle the beginning of object field processing during JSON object expansion. It is called by the JSON parser when it encounters the start of an object field (key-value pair). The function examines the current lexical level and token type to determine how to handle the upcoming value. For top-level fields (lex_level == 1), it sets up state to either normalize string values to text or capture the start position of non-string values for later processing.

## Parameters / Member Variables
- `state`: Void pointer to EachState structure containing parsing context and result storage
- `fname`: Character pointer to the field name (key) being processed
- `isnull`: Boolean indicating whether the field name is null

## Dependencies
- Functions called/Symbols referenced:
  - [EachState](../E/EachState.md) (state structure casting)
  - JSON_TOKEN_STRING (token type comparison)
  - JSON_SUCCESS (return value)
- Called from (representative examples):
  - [each_worker](each_worker.md) (registered as object_field_start callback)

## Notes and Other Information
- Located at src/backend/utils/adt/jsonfuncs.c:2096-2117
- Static function (internal callback implementation)
- Part of the JsonSemAction callback system for JSON parsing
- Only processes fields at the top level of JSON objects (lex_level == 1)
- Sets next_scalar flag for string tokens when normalize_results is enabled
- Captures result_start position for non-string values
- Returns JSON_SUCCESS to indicate successful processing
- Works in conjunction with each_object_field_end to handle complete field processing
- Critical component in the JSON object expansion pipeline

## Simplified Source

```c
static JsonParseErrorType
each_object_field_start(void *state, char *fname, bool isnull)
{
    EachState *each_state = (EachState *) state;

    // Only process top-level object fields
    if (each_state->lex->lex_level == 1)
    {
        // Handle string values for text normalization
        if (each_state->normalize_results &&
            each_state->lex->token_type == JSON_TOKEN_STRING)
        {
            // Flag for string value normalization in field_end handler
            each_state->next_scalar = true;
        }
        else
        {
            // Save starting position of non-string values
            each_state->result_start = each_state->lex->token_start;
        }
    }

    return JSON_SUCCESS;
}
```