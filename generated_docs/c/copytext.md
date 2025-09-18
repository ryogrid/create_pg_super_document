# copytext

## Location
src/tutorial/funcs.c: 64 - 89

## Overview
A PostgreSQL C function that creates a deep copy of a text value, demonstrating proper handling of PostgreSQL's variable-length data types (varlena) and memory management.

## Definition
```c
Datum copytext(PG_FUNCTION_ARGS)
```

## Detailed Description
The `copytext` function is a PostgreSQL C function that creates a complete copy of a text input parameter. This function demonstrates the complexities of working with PostgreSQL's variable-length data types (varlena), including proper size calculation, memory allocation, and data copying. The function handles both regular and compressed/short datum formats transparently using PostgreSQL's VARSIZE and VARDATA macros. It allocates new memory using `palloc`, sets the appropriate header information, and copies the actual text data using `memcpy`.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL macro that provides access to function arguments and context information
  - First argument: A text value accessed via `PG_GETARG_TEXT_PP(0)` (supports both regular and compressed formats)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TEXT_PP`: Macro to extract a text argument (handles both packed and unpacked formats)
  - `[text](../t/text.md)`: PostgreSQL's variable-length text data type
  - `[palloc](../p/palloc.md)`: PostgreSQL's memory allocation function
  - `VARSIZE_ANY_EXHDR`: Macro to get the size excluding header from any varlena format
  - `VARHDRSZ`: Constant for the size of a full varlena header
  - `SET_VARSIZE`: Macro to set the total size in the varlena header
  - `VARDATA`: Macro to get pointer to the data portion of a varlena
  - `VARDATA_ANY`: Macro to get data pointer from any varlena format
  - `memcpy`: Standard C function for memory copying
  - `PG_RETURN_TEXT_P`: Macro to return a text value
  - `PG_FUNCTION_INFO_V1`: Macro for function metadata (referenced at line 87)
- Called from (representative examples):
  - `[makepoint](../m/makepoint.md)`: Referenced from the makepoint function context

## Notes and Other Information
- Located in `src/tutorial/funcs.c:64-89`
- This is a tutorial example function demonstrating advanced varlena handling
- Comprehensive comments explain the intricacies of PostgreSQL's variable-length data handling
- Demonstrates proper memory allocation with full-length headers
- Shows how to handle both compressed (short) and uncompressed datum formats
- Uses `memcpy` for efficient data copying between varlena structures
- Critical for understanding PostgreSQL's internal data representation and memory management
- Follows PostgreSQL's version 1 calling convention