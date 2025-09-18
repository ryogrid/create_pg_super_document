# uuid_out

## Location
src/backend/utils/adt/uuid.c: 53 - 94

## Overview
PostgreSQL output function that converts internal UUID binary format to its standard string representation with hyphens.

## Definition
```c
Datum uuid_out(PG_FUNCTION_ARGS)
```

## Detailed Description
The `uuid_out` function is a PostgreSQL output function that converts a UUID from its internal binary format (`pg_uuid_t`) to the standard string representation. The output format follows the canonical UUID string format: 8-4-4-4-12 hexadecimal digits separated by hyphens (e.g., "550e8400-e29b-41d4-a716-446655440000").

The function manually converts each byte of the UUID data to hexadecimal characters using a lookup table approach for efficiency. It carefully inserts hyphens at the correct positions (after bytes 4, 6, 8, and 10) to produce the standard UUID format.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to:
  - The input UUID (retrieved via `PG_GETARG_UUID_P(0)`)
  - Function call context information

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation for output string)
  - `PG_GETARG_UUID_P` (argument retrieval macro for UUID)
  - `PG_RETURN_CSTRING` (return value macro for C string)
- Constants used:
  - `UUID_LEN` (length of UUID in bytes, typically 16)
- Types used:
  - [pg_uuid_t](../p/pg_uuid_t.md) (internal UUID structure)
- Called from:
  - PostgreSQL type system (automatically during type output)

## Notes and Other Information
- Uses a static hex character lookup table for efficient byte-to-hex conversion
- Allocates exactly the right amount of memory: `2 * UUID_LEN + 5` bytes (32 hex chars + 4 hyphens + null terminator)
- The hyphen placement follows the UUID standard: positions after bytes 4, 6, 8, and 10
- Output format is always lowercase hexadecimal
- This is a PostgreSQL internal function registered in the type system for UUID output
- The function is deterministic and produces consistent string representations