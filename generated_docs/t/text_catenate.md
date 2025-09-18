# text_catenate

## Location
src/backend/utils/adt/varlena.c: 765 - 805

## Overview
Internal static function that performs the actual concatenation of two text values, handling memory allocation and data copying.

## Definition
```c
static text *text_catenate(text *t1, text *t2)
```

## Detailed Description
The `text_catenate` function is the core implementation for text concatenation in PostgreSQL. It's designed as a static helper function that can be reused by various text concatenation operations throughout the codebase. The function handles the low-level details of memory allocation, size calculation, and data copying required to create a new text value from two input text values.

The function is optimized to work with arguments in short-header form but requires that they are not compressed or out-of-line (stored externally). It performs careful length calculations with paranoia checks to prevent negative lengths, allocates the appropriate amount of memory for the result, and efficiently copies the data from both input texts into the new result text.

The implementation uses PostgreSQL's variable-length data structures and follows the platform's memory management patterns using `palloc` for allocation.

## Parameters / Member Variables
- `t1`: First text value to concatenate
- `t2`: Second text value to concatenate

## Dependencies
- Functions called/Symbols referenced:
  - `VARSIZE_ANY_EXHDR`: Macro to get the size of variable-length data excluding header
  - [palloc](../p/palloc.md): PostgreSQL memory allocation function
  - `SET_VARSIZE`: Macro to set the size of a variable-length object
  - `VARDATA`: Macro to get pointer to the data portion of a variable-length object
  - `VARDATA_ANY`: Macro to get data pointer for any variable-length format
  - `memcpy`: Standard C library function for memory copying
  - `VARHDRSZ`: Constant representing variable-length header size

- Called from (representative examples):
  - [textcat](textcat.md): Main text concatenation function
  - [text_overlay](text_overlay.md): Text overlay/replacement function
  - `DatumGetVarStringPP`: Variable string processing function

## Notes and Other Information
- This is a static internal function, not directly accessible from SQL
- Includes paranoia checks for negative lengths, setting them to 0 instead of throwing errors
- The function comment suggests that throwing an error for negative lengths might be more appropriate
- Efficiently handles empty strings by checking lengths before performing memory copies
- Follows PostgreSQL's conventions for variable-length data structure manipulation
- The result must be freed by the caller using PostgreSQL's memory management system