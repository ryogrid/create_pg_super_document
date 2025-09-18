# get_scalar

## Location
[src/backend/utils/adt/jsonfuncs.c:1443-1485](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L1443-L1485)

## Overview
A static callback function used during JSON parsing to handle scalar values (strings, numbers, booleans, null), responsible for capturing and properly formatting scalar results during path-based JSON extraction operations.

## Definition
```c
static JsonParseErrorType get_scalar(void *state, char *token, JsonTokenType tokentype)
```

## Detailed Description
The `get_scalar` function is a JSON parser callback that processes scalar values encountered during JSON parsing operations. It handles the final stage of value extraction, dealing with different scalar types and applying appropriate formatting based on normalization settings.

The function handles several distinct scenarios:
- **Whole-object match**: When extracting an entire JSON document that is a scalar value, it captures the complete input text
- **String normalization**: For JSON strings, it can either capture the raw JSON representation or extract the de-escaped string content
- **Null handling**: Properly represents JSON null values as PostgreSQL NULL when normalization is enabled
- **Deferred string processing**: Handles cases where string de-escaping was requested by previous callbacks

The function implements different text capture strategies depending on whether normalization is required and what type of scalar value is being processed. For normalized strings, it uses the provided token parameter which contains the already de-escaped string content.

## Parameters / Member Variables
- `state`: A void pointer that is cast to `GetState *`, containing the parsing state including lexical analyzer, normalization flags, and result tracking
- `token`: A C string containing the de-escaped token value for strings, or the raw token value for other types
- `tokentype`: A JsonTokenType enumeration indicating the type of scalar token being processed (string, number, boolean, null)

## Dependencies
- Functions called/Symbols referenced:
  - [GetState](../G/GetState.md) (struct type for casting state parameter)
  - [JsonTokenType](../J/JsonTokenType.md) (parameter type for token classification)
  - JSON_TOKEN_STRING (token type constant for string detection)
  - JSON_TOKEN_NULL (token type constant for null detection)
  - cstring_to_text_with_len (converts C string with specified length to PostgreSQL text)
  - cstring_to_text (converts null-terminated C string to PostgreSQL text)
  - JSON_SUCCESS (success return constant)
- Called from (representative examples):
  - [get_worker](get_worker.md) (JSON extraction worker function)
  - JsObjectFree (JSON object processing)

## Notes and Other Information
- This function handles both normalized and raw scalar value extraction based on configuration
- String de-escaping is performed by the JSON lexer before this function receives the token
- The function includes a comment about "hokey" whitespace handling when capturing whole scalar documents
- Whitespace after scalar tokens is suppressed in whole-object matches, but whitespace before is preserved
- The `next_scalar` flag is used for deferred string processing and is automatically reset after use
- Null values are properly represented as PostgreSQL NULL (text *) NULL when normalization is enabled
- The function is the final callback in the JSON extraction pipeline for scalar value processing