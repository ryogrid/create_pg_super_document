# okeys_object_field_start

## Location
[src/backend/utils/adt/jsonfuncs.c:784-806](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L784-L806)

## Overview
A semantic action callback function used during JSON parsing to collect object field names (keys) for the json_object_keys function.

## Definition

```c
static JsonParseErrorType
okeys_object_field_start(void *state, char *fname, bool isnull)
```
## Detailed Description
This function serves as a semantic action callback in PostgreSQL's JSON parser framework. It is specifically designed to capture object field names during JSON parsing for the json_object_keys function. The function operates only on the top-level object (lex_level == 1) and dynamically grows its storage array as needed to accommodate all discovered keys.

The function implements a selective key collection strategy, ignoring nested object keys and focusing solely on the outermost object's field names. It manages memory allocation by doubling the result array size when capacity is exceeded.

## Parameters / Member Variables
- : Void pointer to OkeysState structure containing parsing state and result storage
- : Character pointer to the field name (key) being processed
- : Boolean indicating whether the field name is null (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [OkeysState](../O/OkeysState.md) (cast from state parameter)
  - [repalloc](../r/repalloc.md) (for dynamic array resizing)
  - [pstrdup](../p/pstrdup.md) (for string duplication)
  - JSON_SUCCESS (return value)
- Called from (representative examples):
  - [json_object_keys](../j/json_object_keys.md) (assigned as semantic action callback)
  - JsObjectFree

## Notes and Other Information
- Only processes top-level object keys (lex_level == 1), ignoring nested structures
- Implements dynamic array growth with doubling strategy for efficiency
- Creates copies of field names using pstrdup to ensure data persistence
- Returns JSON_SUCCESS to indicate successful processing
- Part of the semantic action framework for JSON parsing in PostgreSQL