# StripnullState

## Location
src/backend/utils/adt/jsonfuncs.c: 283 - 288

## Overview
A state structure used by PostgreSQL's json_strip_nulls function to maintain parsing state and output buffer during the process of removing null values from JSON data.

## Definition
```c
typedef struct StripnullState
{
    JsonLexContext *lex;
    StringInfo  strval;
    bool        skip_next_null;
} StripnullState;
```

## Detailed Description
StripnullState manages the state required for the json_strip_nulls operation, which removes all null values from JSON objects and arrays. This structure coordinates between JSON parsing and output string construction, maintaining flags to control when null values should be omitted from the resulting JSON string.

The structure works with PostgreSQL's JSON parsing infrastructure to selectively copy non-null elements to the output buffer. It handles the complex logic required to properly format JSON while skipping null values, ensuring that the resulting JSON remains syntactically valid after null removal.

## Parameters / Member Variables
- `lex`: Pointer to JsonLexContext for JSON lexical analysis and parsing operations
- `strval`: StringInfo buffer for accumulating the output JSON string with nulls removed
- `skip_next_null`: Boolean flag indicating whether the next encountered null value should be skipped in the output

## Dependencies
- Functions called/Symbols referenced:
  - [JsonLexContext](../J/JsonLexContext.md) (JSON parsing and lexical analysis)
  - StringInfo (string buffer management)
- Called from (representative examples):
  - [sn_object_start](../s/sn_object_start.md) (strip nulls object start handler)
  - [sn_object_end](../s/sn_object_end.md) (strip nulls object end handler)
  - [sn_array_start](../s/sn_array_start.md) (strip nulls array start handler)
  - [sn_array_end](../s/sn_array_end.md) (strip nulls array end handler)
  - [sn_object_field_start](../s/sn_object_field_start.md) (strip nulls object field start handler)
  - [sn_array_element_start](../s/sn_array_element_start.md) (strip nulls array element start handler)
  - [sn_scalar](../s/sn_scalar.md) (strip nulls scalar value handler)
  - [json_strip_nulls](../j/json_strip_nulls.md) (main function using this state)

## Notes and Other Information
- Specifically designed for the json_strip_nulls functionality in PostgreSQL
- The skip_next_null flag is crucial for handling cases where null values appear in different JSON contexts (objects vs arrays)
- Works with JSON parsing callbacks (sn_* functions) to selectively build output without null values
- Ensures proper JSON syntax is maintained even after removing null elements
- The StringInfo buffer efficiently accumulates the filtered JSON output during parsing
- Essential for maintaining parsing state across multiple callback invocations during JSON traversal
- Used exclusively in src/backend/utils/adt/jsonfuncs.c for JSON null-stripping operations