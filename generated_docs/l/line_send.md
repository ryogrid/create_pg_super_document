# line_send

## Location
src/backend/utils/adt/geo_ops.c: 1061 - 1082

## Overview
Converts a LINE data structure to binary format for transmission or storage. This function serves as the binary output function for the PostgreSQL line data type.

## Definition
```c
Datum line_send(PG_FUNCTION_ARGS)
```

## Detailed Description
The `line_send` function is the binary output converter for PostgreSQL's line geometric data type. It takes an internal LINE structure and serializes the A, B, and C coefficients into a binary format suitable for transmission over the wire protocol or storage in binary format.

The function creates a StringInfo buffer and writes the three floating-point coefficients sequentially using PostgreSQL's standard binary encoding for float8 values. This ensures platform-independent representation and maintains full precision of the coefficients.

The binary format is more efficient than text representation for network transmission and provides exact representation without any formatting-related precision loss.

## Parameters / Member Variables
- `line`: Input LINE structure pointer containing A, B, C coefficients to serialize
- Returns: `Datum` containing bytea with serialized binary data

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_LINE_P`: Extracts LINE pointer from function arguments
  - `[pq_begintypsend](../p/pq_begintypsend.md)`: Initializes StringInfo buffer for binary output
  - `[pq_sendfloat8](../p/pq_sendfloat8.md)`: Writes float8 value to binary buffer
  - `[pq_endtypsend](../p/pq_endtypsend.md)`: Finalizes binary buffer and returns bytea
  - `PG_RETURN_BYTEA_P`: Returns bytea as Datum
- Called from (representative examples):
  - PostgreSQL binary protocol system (no direct function references found)

## Notes and Other Information
- Part of PostgreSQL's geometric data type system in `src/backend/utils/adt/geo_ops.c`
- Counterpart to `line_recv` for binary serialization/deserialization
- Uses PostgreSQL's standard binary encoding functions for platform independence
- More efficient than text representation for network and storage operations
- Memory management handled automatically by StringInfo and pq_endtypsend
- Line numbers: 1061-1082 in geo_ops.c