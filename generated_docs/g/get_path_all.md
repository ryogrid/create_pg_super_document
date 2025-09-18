# get_path_all

## Location
[src/backend/utils/adt/jsonfuncs.c:1022-1100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L1022-L1100)

## Overview
Common worker function that extracts values from JSON documents by following an array of path components, supporting both JSON and text output formats.

## Definition
```c
static Datum get_path_all(FunctionCallInfo fcinfo, bool as_text)
```

## Detailed Description
This is the core implementation function for JSON path extraction operations in PostgreSQL. It processes a path specified as an array of text values, where each element can represent either an object field name or an array index. The function intelligently handles both string keys for object navigation and numeric indices for array access.

The function converts path components to both string and integer representations, using INT_MIN as a sentinel value for non-numeric path components. It then delegates to the `get_worker` function to perform the actual JSON parsing and extraction. The `as_text` parameter determines whether the result should be returned as JSON or converted to text format.

## Parameters / Member Variables
- `fcinfo`: PostgreSQL function call information structure containing the JSON document and path array
- `as_text`: Boolean flag determining output format (true for text, false for JSON)
- Local variables:
  - `json`: Input JSON document as text
  - `path`: Array of path components 
  - `tpath`: Array of string representations of path components
  - `ipath`: Array of integer representations of path components
  - `npath`: Number of path components

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TEXT_PP` - PostgreSQL macro to get text argument
  - `PG_GETARG_ARRAYTYPE_P` - PostgreSQL macro to get array argument
  - [array_contains_nulls](../a/array_contains_nulls.md) - Function to check for NULL elements in array
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md) - Function to deconstruct PostgreSQL array
  - `TextDatumGetCString` - Function to convert text datum to C string
  - `strtoint` - PostgreSQL function to parse integers safely
  - [get_worker](get_worker.md) - Common worker function for JSON extraction operations
  - `PG_RETURN_TEXT_P` - PostgreSQL macro to return text result
  - `PG_RETURN_NULL` - PostgreSQL macro to return NULL
- Called from (representative examples):
  - [json_extract_path](../j/json_extract_path.md) - JSON path extraction returning JSON
  - [json_extract_path_text](../j/json_extract_path_text.md) - JSON path extraction returning text
  - `JsObjectFree` - JSON object cleanup function

## Notes and Other Information
- Returns NULL if any path component is NULL, following the semantics of the -> operator
- Converts numeric path components to integers for array indexing, using INT_MIN as a sentinel for non-numeric components
- Supports mixed navigation through objects and arrays in a single path
- Uses `get_worker` as the underlying JSON parsing engine
- The function handles both the JSON and text variants of path extraction through the `as_text` parameter
- Part of PostgreSQL's JSON path extraction infrastructure, providing the common implementation for multiple user-facing functions
- Includes comprehensive error handling for malformed paths and invalid array structures