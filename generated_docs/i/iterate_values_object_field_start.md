# iterate_values_object_field_start

## Location
src/backend/utils/adt/jsonfuncs.c: 5761 - 5781

## Overview
An auxiliary callback function for JSON parsing that processes object field names (keys) and conditionally invokes a user-defined action based on key flags.

## Definition
```c
static JsonParseErrorType iterate_values_object_field_start(void *state, char *fname, bool isnull)
```

## Detailed Description
This function serves as a JSON parser callback handler specifically for object field names encountered during JSON parsing. It is called when the JSON parser encounters the start of an object field (i.e., the key portion of a key-value pair). The function checks if the jtiKey flag is set in the parsing state, and if so, it creates a copy of the field name string and passes it to the user-defined action callback. This allows selective processing of JSON object keys when building search vectors or performing other operations that need access to field names.

## Parameters / Member Variables
- `state`: Pointer to IterateJsonStringValuesState containing parsing context and callback information
- `fname`: String containing the object field name (key)
- `isnull`: Boolean indicating if the field name is null (not used in current implementation)

## Dependencies
- Functions called/Symbols referenced:
  - pstrdup (to create a copy of the field name)
  - strlen (for calculating field name length)
- Called from (representative examples):
  - iterate_json_values (registered as object_field_start callback)
  - JsObjectFree

## Notes and Other Information
The function duplicates the field name string using pstrdup before passing it to the action callback, ensuring the callback receives a properly allocated string that won't be invalidated by the parser. The isnull parameter is currently not utilized in the implementation. The function always returns JSON_SUCCESS to indicate successful processing. This callback works in conjunction with iterate_values_scalar to provide complete coverage of JSON elements that might be relevant for text search operations.