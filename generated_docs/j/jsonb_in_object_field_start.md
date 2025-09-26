# jsonb_in_object_field_start

## Location
src/backend/utils/adt/jsonb.c: 331 - 348

## Overview
A callback function used during JSONB parsing to handle the start of object field names, processing and validating field keys before their corresponding values.

## Definition

```c
static JsonParseErrorType
jsonb_in_object_field_start(void *pstate, char *fname, bool isnull)
```
## Detailed Description
This function serves as a callback handler in the JSONB input parsing pipeline. When the JSON parser encounters an object field name (the key part of a key-value pair), this function is invoked to process and validate the field name. It creates a JsonbValue structure for the field name, validates the string length, and pushes a WJB_KEY token onto the JSONB parse state stack.

The function is more complex than the simple start/end callbacks as it handles actual data content rather than just structural elements. It performs validation to ensure the field name meets PostgreSQL's constraints and properly formats the key for internal storage in the JSONB structure.

## Parameters / Member Variables
- : A void pointer that is cast to JsonbInState*, containing the current parsing state and configuration
- : A null-terminated string containing the field name (object key)
- : Boolean indicating whether the field name is null (though the function asserts fname is not NULL)

## Dependencies
- Functions called/Symbols referenced:
  - JsonbValue (value structure for JSONB data)
  - jbvString (enum value indicating string type)
  - checkStringLen (validates string length constraints)
  - pushJsonbValue (pushes parsing tokens onto the state stack)
  - WJB_KEY (token constant indicating object key)
  - JSON_SUCCESS (return value indicating successful parsing)
  - JSON_SEM_ACTION_FAILED (return value for validation failures)
- Called from (representative examples):
  - jsonb_from_cstring (main JSONB input function)
  - datum_to_jsonb_internal (internal conversion function)

## Notes and Other Information
- This is a static function internal to the jsonb.c module
- Part of the callback-based JSON parsing architecture in PostgreSQL
- Contains assertion that fname is not NULL, indicating this constraint is enforced by the caller
- Performs length validation through checkStringLen to prevent oversized keys
- Returns JSON_SEM_ACTION_FAILED if string length validation fails
- Sets up the JsonbValue structure with string type and length information
- Works in conjunction with value parsing callbacks to complete key-value pair processing
- Critical for maintaining JSONB key constraints and internal format requirements