# json_unique_object_field_start

## Location
[src/backend/utils/adt/json.c:1639-1663](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L1639-L1663)

## Overview
A callback function used during JSON parsing to handle the start of object fields while checking for unique field names within JSON objects.

## Definition

```c
static JsonParseErrorType
json_unique_object_field_start(void *_state, char *field, bool isnull)
```
## Detailed Description
This function is a specialized JSON parsing callback that ensures object field names are unique within their containing objects. It operates as part of the JSON validation framework and is called when the parser encounters the beginning of an object field. The function maintains a stack-based tracking system to monitor object nesting levels and uses a key collision detection mechanism to identify duplicate field names. When a duplicate is found, it marks the parsing state as non-unique and cleans up the object tracking stack.

## Parameters / Member Variables
- `*_state`: Void pointer to JsonUniqueParsingState containing the parsing context and uniqueness tracking information
- `*field`: Character pointer to the field name being processed
- `isnull`: Boolean indicating whether the field value is null (currently unused in the implementation)
## Dependencies
- Functions called/Symbols referenced:
  - [json_unique_check_key](json_unique_check_key.md)
  - [pfree](../p/pfree.md)
- Data types referenced:
  - [JsonUniqueParsingState](../J/JsonUniqueParsingState.md)
  - [JsonUniqueStackEntry](../J/JsonUniqueStackEntry.md)
  - JSON_SUCCESS
- Called from (representative examples):
  - [json_validate](json_validate.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the json.c source file
- The function follows the JsonParseErrorType callback signature required by the JSON parsing framework
- When uniqueness is violated, the function performs cleanup by popping and freeing all stack entries
- The function returns JSON_SUCCESS in all cases, as the uniqueness violation is recorded in the state rather than reported as a parse error
- Part of PostgreSQL's JSON validation infrastructure for ensuring well-formed JSON objects

## Simplified Source

```c
static JsonParseErrorType
json_unique_object_field_start(void *_state, char *field, bool isnull)
{
    JsonUniqueParsingState *state = _state;
    JsonUniqueStackEntry *entry;

    // Skip if uniqueness checking is disabled
    if (!state->unique)
        return JSON_SUCCESS;

    // Check for key collision in current object
    if (json_unique_check_key(&state->check, field, state->stack->object_id))
        return JSON_SUCCESS;

    // Mark as non-unique and cleanup stack
    state->unique = false;
    while ((entry = state->stack))
    {
        state->stack = entry->parent;
        pfree(entry);
    }

    return JSON_SUCCESS;
}
```