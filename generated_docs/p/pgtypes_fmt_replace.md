# pgtypes_fmt_replace

## Location
src/interfaces/ecpg/pgtypeslib/common.c: 30 - 144

## Overview
A comprehensive format replacement function in the PostgreSQL ECPG pgtypeslib that handles the conversion and formatting of various data types into string representations for output formatting.

## Definition
```c
int pgtypes_fmt_replace(union un_fmt_comb replace_val, int replace_type, char **output, int *pstr_len)
```

## Detailed Description
`pgtypes_fmt_replace` is a central formatting function that handles the conversion of various PostgreSQL data types into their string representations. The function supports multiple data types including strings (both constant and mallocd), characters, doubles, and various integer types with different formatting options (zero-padded, left-padded, etc.).

The function operates by examining the `replace_type` parameter to determine how to process the input value, then formats the value appropriately and writes it to the output buffer. It manages memory allocation internally for numeric conversions and ensures proper buffer bounds checking to prevent overflows.

Key features include:
- Support for multiple data type conversions
- Memory management for dynamically allocated strings
- Buffer overflow protection
- Consistent error handling with return codes
- Various numeric formatting options (zero-padding, field width control)

## Parameters / Member Variables
- `replace_val`: A union containing the value to be formatted, with the actual member used determined by `replace_type`
- `replace_type`: An integer constant specifying the type of data and formatting to apply
- `output`: A pointer to a char pointer that points to the current position in the output buffer; updated to point past the written data
- `pstr_len`: A pointer to an integer containing the remaining space in the output buffer; updated to reflect remaining space after writing

## Dependencies
- Functions called/Symbols referenced:
  - `[pgtypes_alloc](pgtypes_alloc.md)` (for temporary buffer allocation)
  - `strlen` (for string length calculation)
  - `memcpy` (for string copying)
  - `free` (for memory deallocation)
  - `snprintf` (for numeric formatting)
  - `strcpy` (for string copying)
  - Various PGTYPES constants and macros:
    - `PGTYPES_TYPE_NOTHING`
    - `PGTYPES_TYPE_STRING_CONSTANT`
    - `PGTYPES_TYPE_STRING_MALLOCED`
    - `PGTYPES_TYPE_CHAR`
    - `PGTYPES_TYPE_DOUBLE_NF`
    - `PGTYPES_TYPE_INT64`
    - `PGTYPES_TYPE_UINT`
    - `PGTYPES_TYPE_UINT_2_LZ`
    - `PGTYPES_TYPE_UINT_2_LS`
    - `PGTYPES_TYPE_UINT_3_LZ`
    - `PGTYPES_TYPE_UINT_4_LZ`
    - `PGTYPES_FMT_NUM_MAX_DIGITS`
    - `INT64_FORMAT`
- Called from (representative examples):
  - `un_fmt_comb`
  - `[dttofmtasc_replace](../d/dttofmtasc_replace.md)`

## Notes and Other Information
- Returns 0 on success, -1 on buffer overflow or other formatting errors, or ENOMEM on memory allocation failure
- Automatically frees mallocd strings after copying them to the output buffer
- Performs bounds checking to prevent buffer overflows
- Handles various numeric formatting options including zero-padding and field width specifications
- The function updates both `output` and `pstr_len` parameters to reflect the new buffer state after writing
- Critical component of the PostgreSQL ECPG formatting system for date, time, and numeric data types
- Located in `src/interfaces/ecpg/pgtypeslib/common.c:30-144`