# my_truncate

## Location
src/test/examples/testlo64.c: 151 - 171

## Overview
A static function that truncates a PostgreSQL large object to a specified 64-bit length using the 64-bit large object API.

## Definition


## Detailed Description
The  function provides functionality to truncate a PostgreSQL large object to a specific size using the 64-bit large object interface. It opens the large object with both read and write permissions, performs the truncation operation using , and properly closes the large object. This function is specifically designed to work with large objects that may exceed 32-bit size limitations by using the 64-bit API variants.

## Parameters / Member Variables
- : Database connection handle for PostgreSQL operations
- : OID of the large object to truncate
- : Target length (64-bit integer) to truncate the large object to

## Dependencies
- Functions called/Symbols referenced:
  - [lo_open](../l/lo_open.md) (PostgreSQL large object opening)
  - [lo_truncate64](../l/lo_truncate64.md) (PostgreSQL 64-bit large object truncation)
  - [lo_close](../l/lo_close.md) (PostgreSQL large object closing)
  - [PQerrorMessage](../P/PQerrorMessage.md) (PostgreSQL error message retrieval)
  - fprintf (formatted output to stderr)
  - pg_int64 (PostgreSQL 64-bit integer type)
  - INV_READ, INV_WRITE (large object access mode constants)
- Called from (representative examples):
  - [main](main.md) (in src/test/examples/testlo64.c:290)

## Notes and Other Information
- This is a static function used in PostgreSQL 64-bit large object test examples
- Part of the testlo64.c file which demonstrates 64-bit large object operations
- Uses the 64-bit large object API (lo_truncate64) to handle large objects exceeding 32-bit size limits
- Opens the large object with both read and write permissions (INV_READ | INV_WRITE)
- Includes error handling with PQerrorMessage to provide detailed error information
- The function name includes 'my_' prefix to distinguish it from system truncate functions
- Essential for testing large object operations that require precise size control in 64-bit environments
- Error messages are output to stderr for debugging purposes