# pg_parse_json_or_errsave

## Location
[src/backend/utils/adt/jsonfuncs.c:517-537](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L517-L537)

## Overview
This function provides error-safe JSON parsing by extending pg_parse_json with comprehensive error handling through PostgreSQL's ErrorSaveContext mechanism.

## Definition

```c
bool
pg_parse_json_or_errsave(JsonLexContext *lex, JsonSemAction *sem,
						 Node *escontext)
```
## Detailed Description
pg_parse_json_or_errsave serves as a wrapper around pg_parse_json that provides enhanced error handling capabilities. Unlike pg_parse_json which returns a JsonParseErrorType, this function returns a boolean success indicator and handles errors through PostgreSQL's error context system. When parsing fails, it either saves error data into the provided ErrorSaveContext (allowing soft error handling) or reports an ERROR (causing transaction abort). This design enables callers to choose between immediate error reporting and deferred error handling based on their error management strategy.

## Parameters / Member Variables
- : JsonLexContext pointer containing the JSON input data and lexical analysis state
- : JsonSemAction pointer defining the semantic actions to execute during JSON parsing
- : Node pointer that may be an ErrorSaveContext for soft error handling, or NULL for immediate error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [pg_parse_json](pg_parse_json.md)
  - [json_errsave_error](../j/json_errsave_error.md)
  - JsonParseErrorType
  - JSON_SUCCESS
- Called from (representative examples):
  - [json_in](../j/json_in.md)
  - [jsonb_from_cstring](../j/jsonb_from_cstring.md)
  - [populate_array_json](populate_array_json.md)
  - [get_json_object_as_hash](../g/get_json_object_as_hash.md)

## Notes and Other Information
This function is essential for PostgreSQL's JSON processing infrastructure where error recovery is critical. It enables parsing operations to continue even when individual JSON values are malformed, which is particularly important in bulk data processing scenarios. The function follows PostgreSQL's standard error handling patterns by supporting both immediate error reporting and soft error collection through the ErrorSaveContext mechanism.

## Simplified Source

```c
bool
pg_parse_json_or_errsave(JsonLexContext *lex, JsonSemAction *sem,
                        Node *escontext)
{
    // Parse JSON using standard parser
    JsonParseErrorType result = pg_parse_json(lex, sem);

    // Handle errors through error context system
    if (result != JSON_SUCCESS) {
        json_errsave_error(result, lex, escontext);
        return false;
    }

    return true;
}
```