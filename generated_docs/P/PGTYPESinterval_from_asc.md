# PGTYPESinterval_from_asc

## Location
src/interfaces/ecpg/pgtypeslib/interval.c: 1003 - 1061

## Overview
Parses a string representation of an interval and converts it into an interval data structure in the PostgreSQL ECPG pgtypes library.

## Definition
interval *PGTYPESinterval_from_asc(char *str, char **endptr)

## Detailed Description
This function converts a string representation of a time interval into a PostgreSQL interval data structure. It supports various interval formats including standard PostgreSQL interval syntax and ISO 8601 interval format. The function performs comprehensive parsing using ParseDateTime for general datetime parsing, followed by either DecodeInterval for PostgreSQL-style intervals or DecodeISO8601Interval for ISO 8601 format. The parsed components are then converted into the internal interval representation using tm2interval.

The function includes robust error handling with proper memory management - it allocates memory for the result interval and cleans up on errors. It validates that the parsed data represents a valid interval (DTK_DELTA type) and sets errno appropriately for error conditions.

## Parameters / Member Variables
- str: Input string containing the interval representation to be parsed. The string length must not exceed MAXDATELEN characters.
- endptr: Optional pointer to a char* where the function will store the address of the first character after the parsed interval. If NULL, parsing stops at the end of the input string.

## Dependencies
- Functions called/Symbols referenced:
  - pgtypes_alloc (memory allocation)
  - ParseDateTime (datetime string parsing)
  - DecodeInterval (PostgreSQL interval format decoding)
  - DecodeISO8601Interval (ISO 8601 interval format decoding)
  - tm2interval (converts tm structure to interval)
  - free (memory deallocation)
- Called from (representative examples):
  - ecpg_get_data (ECPG data retrieval)
  - main (in various test programs)
  - Client applications using ECPG interval types

## Notes and Other Information
- Returns NULL on parsing errors and sets errno to PGTYPES_INTVL_BAD_INTERVAL
- The returned interval pointer must be freed using PGTYPESinterval_free when no longer needed
- Supports multiple interval formats including PostgreSQL native and ISO 8601
- Input string length is limited to MAXDATELEN characters for security
- Part of the ECPG pgtypes library providing client-side PostgreSQL data type support
- Uses internal parsing infrastructure shared with PostgreSQL server