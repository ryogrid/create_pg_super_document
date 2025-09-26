# populate_array_json

## Location
[src/backend/utils/adt/jsonfuncs.c:2787-2822](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L2787-L2822)

## Overview
Parses a JSON string representing an array and populates a PostgreSQL array structure using callback-based JSON parsing.

## Definition

```c
static bool
populate_array_json(PopulateArrayContext *ctx, const char *json, int len)
```
## Detailed Description
This function orchestrates the parsing of JSON array data by setting up a JSON lexical context and configuring semantic action callbacks for different JSON parsing events. It creates a PopulateArrayState to track parsing progress and configures callbacks for object starts, array ends, array element starts/ends, and scalar values. The function uses PostgreSQL's JSON parser infrastructure to systematically process the JSON string and populate the target array structure. After parsing completion, it validates that the array dimensions have been properly determined and cleans up the lexical context.

## Parameters / Member Variables
- : PopulateArrayContext pointer containing the target array information and error context
- : A character pointer to the JSON string to be parsed
- : The length of the JSON string

## Dependencies
- Functions called/Symbols referenced:
  - [PopulateArrayContext](../P/PopulateArrayContext.md) (context structure)
  - [PopulateArrayState](../P/PopulateArrayState.md) (state structure)
  - [JsonSemAction](../J/JsonSemAction.md) (semantic action structure)
  - [makeJsonLexContextCstringLen](../m/makeJsonLexContextCstringLen.md) (lexical context creation)
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md) (database encoding function)
  - [populate_array_object_start](populate_array_object_start.md) (object start callback)
  - [populate_array_array_end](populate_array_array_end.md) (array end callback)
  - [populate_array_element_start](populate_array_element_start.md) (element start callback)
  - [populate_array_element_end](populate_array_element_end.md) (element end callback)
  - [populate_array_scalar](populate_array_scalar.md) (scalar value callback)
  - [pg_parse_json_or_errsave](pg_parse_json_or_errsave.md) (JSON parser function)
  - [freeJsonLexContext](../f/freeJsonLexContext.md) (cleanup function)
  - SOFT_ERROR_OCCURRED (error checking macro)
- Called from (representative examples):
  - JsObjectFree
  - [populate_array](populate_array.md)

## Notes and Other Information
- This is a static function within jsonfuncs.c, serving as an internal implementation detail
- The function returns false on parsing errors, true on success
- Uses PostgreSQL's error-safe parsing infrastructure (pg_parse_json_or_errsave) to handle malformed JSON gracefully
- Expects array dimensions (ctx->ndims and ctx->dims) to be determined during parsing
- Memory management includes proper cleanup of the JSON lexical context regardless of parsing outcome
- Part of PostgreSQL's JSON-to-array conversion functionality, supporting various JSON array structures
- The semantic action callbacks work together to handle nested JSON structures and convert them to PostgreSQL array format