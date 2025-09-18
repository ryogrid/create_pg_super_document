# concat_text

## Location
src/tutorial/funcs.c: 90 - 109

## Overview
A PostgreSQL C function that concatenates two text values into a single text result, demonstrating advanced variable-length data manipulation and memory management in PostgreSQL.

## Definition
```c
Datum concat_text(PG_FUNCTION_ARGS)
```

## Detailed Description
The `concat_text` function is a PostgreSQL C function that takes two text arguments and concatenates them into a single text result. This function showcases sophisticated handling of PostgreSQL's variable-length data types (varlena), including size calculation for multiple inputs, proper memory allocation for the combined result, and sequential memory copying. The function calculates the total size needed for both input texts plus the header, allocates appropriate memory, and uses two `memcpy` operations to concatenate the data portions sequentially.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL macro that provides access to function arguments and context information
  - First argument: A text value accessed via `PG_GETARG_TEXT_PP(0)`
  - Second argument: A text value accessed via `PG_GETARG_TEXT_PP(1)`

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TEXT_PP`: Macro to extract text arguments (supports both packed and unpacked formats)
  - [text](../t/text.md): PostgreSQL's variable-length text data type
  - `int32`: Standard PostgreSQL integer type for size calculations
  - `VARSIZE_ANY_EXHDR`: Macro to get the data size excluding header from any varlena format
  - `VARHDRSZ`: Constant for the size of a full varlena header
  - [palloc](../p/palloc.md): PostgreSQL's memory allocation function
  - `SET_VARSIZE`: Macro to set the total size in the varlena header
  - `VARDATA`: Macro to get pointer to the data portion of a varlena
  - `VARDATA_ANY`: Macro to get data pointer from any varlena format
  - `memcpy`: Standard C function for memory copying (used twice for concatenation)
  - `PG_RETURN_TEXT_P`: Macro to return a text value
  - `PG_FUNCTION_INFO_V1`: Macro for function metadata (referenced at line 107)
- Called from (representative examples):
  - [copytext](copytext.md): Referenced from the copytext function context

## Notes and Other Information
- Located in `src/tutorial/funcs.c:90-109`
- This is a tutorial example function demonstrating text concatenation with varlena types
- Efficiently calculates combined size before allocation to minimize memory usage
- Uses two sequential `memcpy` operations with pointer arithmetic for concatenation
- Demonstrates proper handling of multiple variable-length inputs
- Shows advanced varlena manipulation beyond simple copying
- Critical example for understanding string operations in PostgreSQL C functions
- Follows PostgreSQL's version 1 calling convention