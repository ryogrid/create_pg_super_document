# set_fname

## Location
src/common/jsonapi.c: 452 - 457

## Overview
Sets the field name for the current lexical level in a JSON lexical context structure.

## Definition


## Detailed Description
The `set_fname` function is a static inline helper function that assigns a field name to the current lexical level within a JSON parsing context. It updates the `fnames` array at the position corresponding to the current `lex_level` in the JSON lexical context's parse stack. This function is part of the JSON parsing infrastructure in PostgreSQL's common library.

## Parameters / Member Variables
- `lex`: Pointer to the JsonLexContext structure containing the parsing state
- `fname`: Character pointer to the field name string to be set

## Dependencies
- Functions called/Symbols referenced:
  - JsonLexContext (structure type)
- Called from (representative examples):
  - pg_parse_json_incremental

## Notes and Other Information
This is a static inline function, meaning it's only accessible within the jsonapi.c file and will likely be inlined by the compiler for performance. The function provides a clean abstraction for managing field names during JSON parsing, helping to track the hierarchical structure of nested JSON objects.