# json_strip_nulls

## Location
src/backend/utils/adt/jsonfuncs.c: 4492 - 4524

## Overview
A PostgreSQL SQL function that removes null-valued fields from JSON objects, returning a new JSON value with null fields stripped out.

## Definition
```c
Datum json_strip_nulls(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the SQL function `json_strip_nulls(json) -> json` which processes JSON input and returns a new JSON value with all null-valued object fields removed. The function works by setting up a complete JSON parsing framework using semantic action callbacks. It creates a lexical context for parsing, initializes a string buffer for output, and configures semantic action callbacks for each JSON construct (objects, arrays, scalars, etc.). The semantic actions work together to reconstruct the JSON while selectively omitting null field values. The parsing is performed by `pg_parse_json_or_ereport`, which walks through the input JSON and invokes the appropriate callbacks. The result is then converted back to a PostgreSQL text datum and returned.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: Input JSON as a `text` datum

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TEXT_PP` - Macro to extract text argument from function args
  - [palloc0](../p/palloc0.md) - PostgreSQL memory allocation function with zero initialization
  - [makeJsonLexContext](../m/makeJsonLexContext.md) - Creates JSON lexical parsing context
  - `makeStringInfo` - Creates expandable string buffer
  - [sn_object_start](../s/sn_object_start.md) - Callback for JSON object start
  - [sn_object_end](../s/sn_object_end.md) - Callback for JSON object end  
  - [sn_array_start](../s/sn_array_start.md) - Callback for JSON array start
  - [sn_array_end](../s/sn_array_end.md) - Callback for JSON array end
  - [sn_scalar](../s/sn_scalar.md) - Callback for JSON scalar values
  - [sn_array_element_start](../s/sn_array_element_start.md) - Callback for JSON array element start
  - [sn_object_field_start](../s/sn_object_field_start.md) - Callback for JSON object field start
  - `pg_parse_json_or_ereport` - Main JSON parsing function with error reporting
  - `cstring_to_text_with_len` - Converts C string to PostgreSQL text datum
  - `PG_RETURN_TEXT_P` - Macro to return text datum from function

- Called from (representative examples):
  - SQL queries using `json_strip_nulls()` function
  - No direct C code references found

## Dependencies
- Structures used:
  - [StripnullState](../S/StripnullState.md) - State structure containing parsing context and output buffer
  - [JsonLexContext](../J/JsonLexContext.md) - JSON lexical analysis context
  - [JsonSemAction](../J/JsonSemAction.md) - Structure containing semantic action callback functions

## Notes and Other Information
This function serves as the entry point and orchestrator for the JSON null-stripping functionality. It demonstrates the PostgreSQL JSON parsing framework architecture, where semantic actions are configured as callbacks and the parser invokes them during JSON traversal. The function is designed to be called from SQL and follows PostgreSQL V1 calling conventions. The null-stripping logic is primarily implemented in the semantic action callbacks, particularly `sn_object_field_start` and `sn_scalar`, making this function a coordinator rather than implementing the core logic directly. The function ensures memory is properly allocated using `palloc0` and follows PostgreSQL memory management patterns.