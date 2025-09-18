# sn_scalar

## Location
src/backend/utils/adt/jsonfuncs.c: 4469 - 4491

## Overview
A callback function used during JSON parsing to handle scalar values, implementing null value skipping and proper formatting for different token types.

## Definition
```c
static JsonParseErrorType sn_scalar(void *state, char *token, JsonTokenType tokentype)
```

## Detailed Description
This function is the core scalar value handler in the JSON null-stripping functionality. It implements the actual null-skipping logic by checking the `skip_next_null` flag that was set by `sn_object_field_start` when a null field value was encountered. When this flag is set, the function asserts that the token is indeed null and skips processing, effectively removing the null value from the output. For non-skipped values, the function handles different token types appropriately: string tokens are escaped for JSON compliance, while other scalar types (numbers, booleans, etc.) are appended directly to the output buffer. This function is essential for the null-stripping feature while maintaining proper JSON formatting.

## Parameters / Member Variables
- `state`: A void pointer that is cast to `StripnullState *` containing the parsing state and output buffer
- `token`: The scalar value as a null-terminated string
- `tokentype`: The type of JSON token being processed (from `JsonTokenType` enum)

## Dependencies
- Functions called/Symbols referenced:
  - `[escape_json](../e/escape_json.md)` - Function to properly escape string values for JSON output
  - `appendStringInfoString` - Function to append string content to the string buffer
  - `[StripnullState](../S/StripnullState.md)` - State structure for null-stripping operations
  - `JSON_SUCCESS` - Success return code constant
  - `[JsonTokenType](../J/JsonTokenType.md)` - Enum type for JSON token classification
  - `JSON_TOKEN_NULL` - Token type constant for null values
  - `JSON_TOKEN_STRING` - Token type constant for string values

- Called from (representative examples):
  - `[json_strip_nulls](../j/json_strip_nulls.md)` - Main function that orchestrates JSON null stripping
  - `JsObjectFree` - Object cleanup function

## Notes and Other Information
This function contains an assertion that verifies null tokens are only skipped when expected, providing debugging assurance for the null-skipping logic. The function handles the critical distinction between string tokens (which need escaping) and other scalar types (numbers, booleans, etc.) that can be appended directly. The `skip_next_null` flag is reset after use, ensuring the state is clean for subsequent parsing. This function represents the culmination of the null-stripping logic initiated by the field start handlers.