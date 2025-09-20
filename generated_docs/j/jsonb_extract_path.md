# jsonb_extract_path

## Location
[src/backend/utils/adt/jsonfuncs.c:1486-1491](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L1486-L1491)

## Overview
A PostgreSQL function that extracts a JSONB value at a specified path, returning the result as JSONB format.

## Definition

```c
struct_array_builtin(path, TEXTOID, &pathtext, &pathnulls, &npath);
```
## Detailed Description
The  function serves as a wrapper that extracts values from JSONB data structures using a path specification. It internally delegates to  with the  parameter set to , ensuring the result is returned in JSONB format rather than text format. This function is typically used when you want to maintain the JSONB data type of the extracted value.

## Parameters / Member Variables
- Uses PostgreSQL's standard function call interface () which contains:

## Dependencies
- Functions called/Symbols referenced:
  - [get_jsonb_path_all](../g/get_jsonb_path_all.md) (with )
- Called from (representative examples):
  - SQL function calls via PostgreSQL's function call interface

## Notes and Other Information
- Located in src/backend/utils/adt/jsonfuncs.c:1486-1491
- This is a thin wrapper function that provides the JSONB-preserving variant of JSONB path extraction
- The actual path extraction logic is implemented in 
- Returns JSONB data type, preserving the original structure and type information of the extracted value