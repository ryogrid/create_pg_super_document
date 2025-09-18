# pg_do_encoding_conversion

## Location
src/backend/utils/mb/mbutils.c: 356 - 468

## Overview
Converts a string from one encoding to another using PostgreSQL's encoding conversion system, handling memory allocation and validation for the conversion process.

## Definition


## Detailed Description
This is the core function for performing encoding conversions in PostgreSQL. It takes a source string and converts it from one character encoding to another, handling various edge cases and optimizations:

- Returns the original string if no conversion is needed (same encodings)
- Handles SQL_ASCII encoding specially as it's compatible with any encoding
- Validates that conversions can only happen within a transaction context
- Finds and invokes the appropriate conversion function for the encoding pair
- Allocates memory conservatively to handle worst-case conversion expansion
- Optimizes memory usage by realloc'ing large results to actual size

The function uses PostgreSQL's function call mechanism to invoke encoding-specific conversion procedures and includes robust error handling for unsupported conversions and memory allocation failures.

## Parameters / Member Variables
- : Source string to convert (unsigned char pointer)
- : Length of the source string in bytes
- : Source encoding identifier (integer constant like PG_UTF8)
- : Destination encoding identifier

## Dependencies
- Functions called/Symbols referenced:
  - pg_verify_mbstr (validates multibyte string)
  - IsTransactionState (checks transaction context)
  - FindDefaultConversionProc (finds conversion function)
  - pg_encoding_to_char (encoding name lookup)
  - MemoryContextAllocHuge (memory allocation)
  - OidFunctionCall6 (invokes conversion function)
  - repalloc (memory reallocation)
- Called from (representative examples):
  - pg_convert (SQL function wrapper)
  - pg_any_to_server (server encoding conversion)
  - pg_server_to_any (client encoding conversion)
  - xml_parse (XML processing)

## Notes and Other Information
- Must be called within a transaction context due to catalog access requirements
- Uses MAX_CONVERSION_GROWTH constant to estimate worst-case memory needs
- Includes overflow protection for very large strings
- For large results (>1MB), optimizes memory usage by shrinking allocated space
- SQL_ASCII encoding is treated specially as universally compatible
- Returns original pointer when no conversion is needed for efficiency