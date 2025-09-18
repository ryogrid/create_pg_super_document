# sn_object_field_start

## Location
src/backend/utils/adt/jsonfuncs.c: 4428 - 4457

## Overview
A callback function used during JSON parsing to handle the start of an object field, managing null value skipping and field name formatting.

## Definition
```c
static JsonParseErrorType sn_object_field_start(void *state, char *fname, bool isnull)
```

## Detailed Description
This function is a critical component of the JSON null-stripping functionality in PostgreSQL. It handles the beginning of object fields during JSON parsing. When a field value is null and should be stripped, the function sets a flag to skip the next null value. For non-null fields, it formats the field name properly by adding commas between fields (when not the first field), re-escaping the field name for JSON compliance, and appending the colon separator. The function intelligently manages JSON object syntax while supporting the null-stripping feature.

## Parameters / Member Variables
- `state`: A void pointer that is cast to `StripnullState *` containing the parsing state and output buffer
- `fname`: The field name as a null-terminated string
- `isnull`: Boolean flag indicating whether the field value is null

## Dependencies
- Functions called/Symbols referenced:
  - `appendStringInfoCharMacro` - Macro to append single characters to the string buffer
  - `escape_json` - Function to properly escape field names for JSON output
  - `StripnullState` - State structure for null-stripping operations
  - `JSON_SUCCESS` - Success return code constant
  - `JsonParseErrorType` - Return type for JSON parsing operations

- Called from (representative examples):
  - `json_strip_nulls` - Main function that orchestrates JSON null stripping
  - `JsObjectFree` - Object cleanup function

## Notes and Other Information
This function implements a sophisticated null-handling strategy by setting the `skip_next_null` flag when a field value is null, allowing the subsequent scalar handler to skip null values. The function must re-escape field names because the original quoted and escaped form is not available at this stage. The comma insertion logic ensures proper JSON object syntax by checking if the previous character is an opening brace `{` to determine if this is the first field.