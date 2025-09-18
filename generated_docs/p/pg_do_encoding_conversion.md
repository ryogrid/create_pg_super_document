# pg_do_encoding_conversion

## Location
[src/backend/utils/mb/mbutils.c:356-468](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L356-L468)

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
  - [pg_verify_mbstr](pg_verify_mbstr.md) (validates multibyte string)
  - [IsTransactionState](../I/IsTransactionState.md) (checks transaction context)
  - [FindDefaultConversionProc](../F/FindDefaultConversionProc.md) (finds conversion function)
  - pg_encoding_to_char (encoding name lookup)
  - [MemoryContextAllocHuge](../M/MemoryContextAllocHuge.md) (memory allocation)
  - OidFunctionCall6 (invokes conversion function)
  - [repalloc](../r/repalloc.md) (memory reallocation)
- Called from (representative examples):
  - [pg_convert](pg_convert.md) (SQL function wrapper)
  - [pg_any_to_server](pg_any_to_server.md) (server encoding conversion)
  - [pg_server_to_any](pg_server_to_any.md) (client encoding conversion)
  - xml_parse (XML processing)

## Notes and Other Information
- Must be called within a transaction context due to catalog access requirements
- Uses MAX_CONVERSION_GROWTH constant to estimate worst-case memory needs
- Includes overflow protection for very large strings
- For large results (>1MB), optimizes memory usage by shrinking allocated space
- SQL_ASCII encoding is treated specially as universally compatible
- Returns original pointer when no conversion is needed for efficiency