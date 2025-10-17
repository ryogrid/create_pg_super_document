# alen_array_element_start

## Location
[src/backend/utils/adt/jsonfuncs.c:1926-1947](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L1926-L1947)

## Overview
A JSON parser callback function that counts array elements at the top level when determining array length.

## Definition
```c
static JsonParseErrorType alen_array_element_start(void *state, bool isnull)
```

## Detailed Description
This function is a callback used by the JSON parser when processing array length operations. It is called at the start of each array element and is responsible for incrementing the count of elements in the array. The function specifically counts elements at level 1 (the top-level array elements) to determine the total length of the JSON array.

Unlike the validation functions (alen_object_start and alen_scalar), this function performs the actual counting work by incrementing the count field in the AlenState structure whenever it encounters an array element at the appropriate nesting level.

## Parameters / Member Variables
- `state`: void pointer that is cast to AlenState structure containing the lexer state and counter information
- `isnull`: boolean flag indicating whether the current array element is null

## Dependencies
- Functions called/Symbols referenced:
  - [AlenState](../A/AlenState.md) (structure type)
  - JSON_SUCCESS (return value constant)
- Called from (representative examples):
  - [json_array_length](../j/json_array_length.md) (main array length function)
  - JsObjectFree (JSON object processing)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the jsonfuncs.c file
- The function performs the core counting logic for array length determination
- Only increments the counter when lex_level == 1, ensuring it counts top-level array elements
- The isnull parameter is provided but not used in the current implementation
- Always returns JSON_SUCCESS as it doesn't perform validation, only counting
- Part of the JSON parsing callback system used throughout PostgreSQL's JSON functionality

## Simplified Source

```c
static JsonParseErrorType
alen_array_element_start(void *state, bool isnull)
{
    AlenState *alen_state = (AlenState *) state;

    // Count top-level array elements (level 1)
    if (alen_state->lex->lex_level == 1)
        alen_state->count++;

    return JSON_SUCCESS;
}
```