# json_unique_object_end

## Location
[src/backend/utils/adt/json.c:1624-1638](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L1624-L1638)

## Overview
Semantic action function for JSON parsing that handles cleanup when finishing JSON object processing during key uniqueness validation.

## Definition

```c
static JsonParseErrorType
json_unique_object_end(void *_state)
```
## Detailed Description
The `json_unique_object_end` function is a semantic action callback used during JSON parsing to handle the completion of JSON objects when key uniqueness validation is enabled. It pops the current object's tracking entry from the parsing state stack and frees the associated memory. This function pairs with `json_unique_object_start` to maintain proper stack management during nested JSON object parsing and ensures clean resource cleanup.

## Parameters / Member Variables
- `_state`: Void pointer to JsonUniqueParsingState structure containing parsing context and uniqueness tracking information

## Dependencies
- Functions called/Symbols referenced:
  - [JsonUniqueParsingState](../J/JsonUniqueParsingState.md) (type cast)
  - [JsonUniqueStackEntry](../J/JsonUniqueStackEntry.md) (type)
  - [pfree](../p/pfree.md)
  - JSON_SUCCESS (return value)
- Called from (representative examples):
  - [json_validate](json_validate.md)

## Notes and Other Information
- Returns JSON_SUCCESS immediately if uniqueness checking is disabled
- Pops the current stack entry and restores parent as current stack head
- Frees allocated memory using pfree to prevent memory leaks
- Part of JSON parsing semantic actions framework
- Static function scope limits visibility to json.c compilation unit
- Essential for implementing JSON object key uniqueness validation
- Works in conjunction with json_unique_object_start to manage object lifecycle
- Maintains proper stack discipline for nested object parsing
- Ensures clean resource management even with deeply nested JSON structures