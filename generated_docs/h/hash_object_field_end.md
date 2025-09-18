# hash_object_field_end

## Location
src/backend/utils/adt/jsonfuncs.c: 3877 - 3927

## Overview
A static JSON parsing callback function that completes object field processing by storing the parsed field value in a hash table entry during JSON-to-hash conversion.

## Definition
```c
static JsonParseErrorType hash_object_field_end(void *state, char *fname, bool isnull)
```

## Detailed Description
This function serves as the completion callback for object field processing in PostgreSQL's JSON parsing framework. It is invoked when the parser finishes processing a field value and is responsible for storing the parsed data in the hash table. Key operations include:

1. **Level filtering**: Only processes top-level fields (lex_level <= 1), consistent with hash_object_field_start
2. **Name length validation**: Rejects field names that are too long (>= NAMEDATALEN) to prevent truncation issues
3. **Hash table insertion**: Creates or updates hash entries using the field name as the key
4. **Value storage**: Stores either the raw JSON text (for complex types) or scalar values
5. **Duplicate handling**: Allows later fields with the same name to override earlier ones

The function works in conjunction with hash_object_field_start to complete the field processing cycle.

## Parameters / Member Variables
- `state`: A void pointer cast to JHashState*, containing parsing state and the target hash table
- `fname`: The field name as a null-terminated string, used as the hash key
- `isnull`: Boolean indicating if the field value is null, used for validation

## Dependencies
- Functions called/Symbols referenced:
  - [JHashState](../J/JHashState.md) (struct type for state management)
  - [JsonHashEntry](../J/JsonHashEntry.md) (struct type for hash table entries)
  - JSON_SUCCESS (return value constant)
  - NAMEDATALEN (maximum identifier length constant)
  - [hash_search](hash_search.md) (hash table lookup/insert function)
  - HASH_ENTER (hash operation mode)
  - JSON_TOKEN_NULL (token type constant)
  - JsonParseErrorType (return type)
- Called from (representative examples):
  - [get_json_object_as_hash](../g/get_json_object_as_hash.md)
  - JsObjectFree

## Notes and Other Information
- This is a static function, only accessible within jsonfuncs.c
- Field names exceeding NAMEDATALEN are silently ignored to maintain exact equality semantics
- Duplicate field names result in the last value overriding previous ones
- Complex JSON structures (arrays/objects) are stored as raw JSON text strings
- Scalar values are stored directly from the saved_scalar field
- The function includes an assertion to validate null consistency between isnull parameter and token type
- Memory allocation for string values uses palloc() and includes proper null termination