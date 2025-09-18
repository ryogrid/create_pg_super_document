# hash_object_field_start

## Location
[src/backend/utils/adt/jsonfuncs.c:3851-3876](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L3851-L3876)

## Overview
A static JSON parsing callback function that handles the start of object field processing during JSON hash table construction, preparing state for subsequent field value processing.

## Definition


## Detailed Description
This function serves as a callback in the JSON parsing framework, specifically designed for building hash tables from JSON objects. It is called when the JSON parser encounters the beginning of an object field. The function performs several key operations:

1. **Level filtering**: Only processes fields at the top level (lex_level <= 1), ignoring nested structures
2. **Token type preservation**: Saves the current token type for later processing
3. **Position tracking**: Records the start position of complex values (arrays/objects) for potential text extraction
4. **State preparation**: Sets up the parsing state for the subsequent field value processing

The function is part of the JSON-to-hash conversion mechanism used in PostgreSQL's JSON functionality.

## Parameters / Member Variables
- : A void pointer that is cast to JHashState*, containing the parsing state and context
- : The field name as a null-terminated string (not used in this function)
- : Boolean indicating if the field name is null (not used in this function)

## Dependencies
- Functions called/Symbols referenced:
  - [JHashState](../J/JHashState.md) (struct type for state management)
  - JSON_SUCCESS (return value constant)
  - JSON_TOKEN_ARRAY_START (token type constant)
  - JSON_TOKEN_OBJECT_START (token type constant)
  - JsonParseErrorType (return type)
- Called from (representative examples):
  - [get_json_object_as_hash](../g/get_json_object_as_hash.md)
  - JsObjectFree

## Notes and Other Information
- This is a static function, meaning it's only accessible within the jsonfuncs.c file
- The function only processes top-level fields (lex_level > 1 check) to avoid deep nesting complexity
- The fname and isnull parameters are not utilized in the current implementation
- Position tracking (save_json_start) is only performed for complex JSON structures (arrays and objects)
- Always returns JSON_SUCCESS, indicating this callback doesn't perform validation that could fail