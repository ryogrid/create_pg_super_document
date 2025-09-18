# get_jsonb_path_all

## Location
[src/backend/utils/adt/jsonfuncs.c:1498-1528](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L1498-L1528)

## Overview
A static function that implements the core logic for extracting values from JSONB data structures using a text array path, with optional text conversion.

## Definition


## Detailed Description
The  function is the core implementation for JSONB path extraction in PostgreSQL. It accepts a JSONB value and an array of text path elements, then navigates through the JSONB structure to extract the value at the specified path. The function includes null-safety checks and can optionally convert the result to text format based on the  parameter. This function serves as the backend implementation for both  and .

## Parameters / Member Variables
- : PostgreSQL function call information containing the input arguments
  - Argument 0: JSONB input data ()
  - Argument 1: ArrayType containing text path elements ()
- : Boolean flag determining output format
  - : Convert result to text representation
  - : Return result in JSONB format

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P (extract JSONB argument)
  - PG_GETARG_ARRAYTYPE_P (extract array argument)
  - [array_contains_nulls](../a/array_contains_nulls.md) (null validation)
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md) (array processing)
  - [jsonb_get_element](../j/jsonb_get_element.md) (core extraction logic)
  - PG_RETURN_DATUM (return result)
- Called from (representative examples):
  - [jsonb_extract_path](../j/jsonb_extract_path.md)
  - [jsonb_extract_path_text](../j/jsonb_extract_path_text.md)
  - JsObjectFree

## Notes and Other Information
- Located in src/backend/utils/adt/jsonfuncs.c:1498-1528
- Implements null-safety: returns NULL if the path array contains any NULL elements
- This mirrors the behavior of nested -> operator applications
- The actual element extraction is delegated to 
- Static function, only accessible within the same compilation unit
- Handles both JSONB and text output formats through the  parameter