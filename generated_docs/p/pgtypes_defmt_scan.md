# pgtypes_defmt_scan

## Location
src/interfaces/ecpg/pgtypeslib/dt_common.c: 2457 - 2518

## Overview
A static helper function that parses and extracts typed values from string input during date/time format string processing in ECPG.

## Definition


## Detailed Description
This function performs the core parsing work for date/time format string processing in the ECPG pgtypes library. It extracts a token from the input string based on the format specification and converts it to the appropriate data type. The function uses the find_end_token helper to determine token boundaries, then performs type-specific parsing and conversion.

The function supports three main data types:
- PGTYPES_TYPE_UINT: Unsigned integers (using strtol)
- PGTYPES_TYPE_UINT_LONG: Unsigned long integers (using strtol)  
- PGTYPES_TYPE_STRING_MALLOCED: Dynamically allocated strings (using pgtypes_strdup)

The parsing process includes whitespace handling, temporary null termination for safe parsing, and proper error detection through errno checking and return values.

## Parameters / Member Variables
- : Union structure to store the parsed value in the appropriate type
- : Integer constant specifying the expected data type (PGTYPES_TYPE_UINT, PGTYPES_TYPE_UINT_LONG, or PGTYPES_TYPE_STRING_MALLOCED)
- : Pointer to the current position in the input string (modified to advance past parsed token)
- : Format string used to determine token boundaries

## Dependencies
- Functions called/Symbols referenced:
  - [find_end_token](../f/find_end_token.md)
  - [pgtypes_strdup](pgtypes_strdup.md)
  - strtol (standard C library function)
  - un_fmt_comb (union type)
  - PGTYPES_TYPE_UINT (constant)
  - PGTYPES_TYPE_UINT_LONG (constant)
  - PGTYPES_TYPE_STRING_MALLOCED (constant)
- Called from (representative examples):
  - [PGTYPEStimestamp_defmt_scan](../P/PGTYPEStimestamp_defmt_scan.md) (multiple times)

## Notes and Other Information
- This is a static function, only accessible within the dt_common.c file
- Returns 0 on success, 1 on error
- Handles blank-padded numbers as acceptable deviation from strict format compliance
- Uses temporary null termination to isolate tokens for safe parsing
- Advances the input string pointer past the successfully parsed token
- Part of the ECPG pgtypes library for date/time manipulation
- Located in src/interfaces/ecpg/pgtypeslib/dt_common.c:2457-2518
- Extensively used by PGTYPEStimestamp_defmt_scan for various format specifier parsing