# jsonb_extract_path_text

## Location
[src/backend/utils/adt/jsonfuncs.c:1492-1497](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L1492-L1497)

## Overview
A PostgreSQL function that extracts a JSONB value at a specified path, returning the result as text format.

## Definition

```c
struct_array_builtin(path, TEXTOID, &pathtext, &pathnulls, &npath);
```
## Detailed Description
The  function serves as a wrapper that extracts values from JSONB data structures using a path specification and converts the result to text format. It internally delegates to  with the  parameter set to , ensuring the result is returned as a text string rather than maintaining the JSONB data type. This function is useful when you need the extracted value as a plain text representation.

## Parameters / Member Variables
- Uses PostgreSQL's standard function call interface () which contains:

## Dependencies
- Functions called/Symbols referenced:
  - [get_jsonb_path_all](../g/get_jsonb_path_all.md) (with )
- Called from (representative examples):
  - SQL function calls via PostgreSQL's function call interface

## Notes and Other Information
- Located in src/backend/utils/adt/jsonfuncs.c:1492-1497
- This is a thin wrapper function that provides the text-converting variant of JSONB path extraction
- The actual path extraction logic is implemented in 
- Returns text data type, converting the extracted JSONB value to its string representation
- Companion function to , differing only in the output format

## Simplified Source
```c
Datum jsonb_extract_path_text(PG_FUNCTION_ARGS) {
    // Simple wrapper: extract JSONB value following specified path
    // Returns result as text (not JSONB)
    return get_jsonb_path_all(fcinfo, true);
}
```