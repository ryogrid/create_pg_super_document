# pgtypes_strdup

## Location
src/interfaces/ecpg/pgtypeslib/common.c: 20 - 29

## Overview
A string duplication wrapper function in the PostgreSQL ECPG pgtypeslib that duplicates strings and provides consistent error handling by setting errno on failure.

## Definition
```c
char *pgtypes_strdup(const char *str)
```

## Detailed Description
`pgtypes_strdup` is a utility function that provides a consistent string duplication interface for the PostgreSQL ECPG pgtypes library. It wraps the standard `strdup` function to create a duplicate copy of a null-terminated string. The function includes error handling by setting the global `errno` variable to `ENOMEM` when memory allocation fails during string duplication, providing a standardized way to handle allocation failures throughout the pgtypes library.

This function is essential for creating independent copies of strings used in PostgreSQL data type operations, ensuring that the original strings remain unchanged and that the copied strings can be safely modified or freed independently.

## Parameters / Member Variables
- `str`: A pointer to the null-terminated string to be duplicated

## Dependencies
- Functions called/Symbols referenced:
  - `strdup` (standard C library function for string duplication)
- Called from (representative examples):
  - [PGTYPESdate_to_asc](../P/PGTYPESdate_to_asc.md)
  - `PGTYPESdate_defmt_asc`
  - [pgtypes_defmt_scan](pgtypes_defmt_scan.md)
  - [PGTYPESinterval_to_asc](../P/PGTYPESinterval_to_asc.md)
  - `un_fmt_comb`
  - [PGTYPEStimestamp_to_asc](../P/PGTYPEStimestamp_to_asc.md)
  - [PGTYPEStimestamp_defmt_asc](../P/PGTYPEStimestamp_defmt_asc.md)

## Notes and Other Information
- Returns a pointer to a newly allocated string containing a copy of the input string on success, or NULL on failure
- Sets `errno` to `ENOMEM` when memory allocation fails during duplication
- The caller is responsible for freeing the returned string using `free()` or appropriate cleanup functions
- Used throughout the pgtypes library for string operations involving PostgreSQL date, timestamp, and interval data types
- Provides consistent memory management semantics across the pgtypes library
- Located in `src/interfaces/ecpg/pgtypeslib/common.c:20-29`