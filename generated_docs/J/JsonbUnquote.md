# JsonbUnquote

## Location
src/backend/utils/adt/jsonb.c: 2166 - 2191

## Overview
Converts a JSONB value to a C-string representation, stripping quotes from scalar strings and converting other scalar types to their string representations.

## Definition
```c
char *JsonbUnquote(Jsonb *jb)
```

## Detailed Description
This utility function converts JSONB values to C-string format with special handling for scalar values. For scalar strings, it strips the surrounding quotes and returns the raw string content. For other scalar types (boolean, numeric, null), it converts them to their appropriate string representations. For non-scalar JSONB values (objects, arrays), it delegates to JsonbToCString for full JSON representation. This function is primarily used in contexts where unquoted string values are needed from JSONB data.

## Parameters / Member Variables
- `jb`: Pointer to the input Jsonb structure to convert

## Dependencies
- Functions called/Symbols referenced:
  - `JB_ROOT_IS_SCALAR` - macro to check if JSONB root is a scalar value
  - `JsonbExtractScalar` - extracts scalar value from JSONB root
  - `pnstrdup` - duplicates string with specified length
  - `pstrdup` - duplicates null-terminated string
  - `DatumGetCString` - converts Datum to C-string
  - `DirectFunctionCall1` - calls another PostgreSQL function directly
  - `numeric_out` - converts numeric to string representation
  - `PointerGetDatum` - wraps pointer as Datum
  - `elog` - logs error messages
  - `JsonbToCString` - converts JSONB to complete JSON string
  - `VARSIZE` - gets size of variable-length data
- Called from (representative examples):
  - `json_populate_type` at src/backend/utils/adt/jsonfuncs.c:3378
  - `PG_RETURN_JSONB_P` at src/include/utils/jsonb.h:425

## Notes and Other Information
- Handles all JSONB scalar types: string, boolean, numeric, and null
- For strings: returns unquoted content using pnstrdup with explicit length
- For booleans: returns "true" or "false" string literals
- For numerics: uses numeric_out for proper formatting
- For null: returns "null" string literal
- For non-scalar values: falls back to full JSON representation
- Throws ERROR for unrecognized JSONB value types
- Memory management: caller is responsible for freeing returned string
- Located in src/backend/utils/adt/jsonb.c:2166-2191