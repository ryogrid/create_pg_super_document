# line_recv

## Location
src/backend/utils/adt/geo_ops.c: 1038 - 1060

## Overview
Converts external binary format to the internal LINE data structure. This function serves as the binary input function for the PostgreSQL line data type.

## Definition
```c
Datum line_recv(PG_FUNCTION_ARGS)
```

## Detailed Description
The `line_recv` function is the binary input converter for PostgreSQL's line geometric data type. It reads binary data from a StringInfo buffer and reconstructs a LINE structure from the serialized floating-point coefficients A, B, and C. This function is used when line data is received over the wire in binary protocol or when reading from binary storage formats.

The function reads three consecutive float8 values representing the line equation coefficients in the standard form Ax + By + C = 0. It performs the same validation as `line_in` to ensure that A and B are not both zero, which would represent an invalid line specification.

The binary format ensures efficient transmission and storage while maintaining full precision of the floating-point coefficients.

## Parameters / Member Variables
- `buf`: StringInfo buffer containing binary data to deserialize
- Returns: `Datum` containing pointer to newly allocated LINE structure

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_POINTER`: Extracts StringInfo pointer from function arguments
  - [palloc](../p/palloc.md): Allocates memory for LINE structure
  - [pq_getmsgfloat8](../p/pq_getmsgfloat8.md): Reads float8 value from binary message buffer
  - `FPzero`: Tests if floating-point value is zero
  - `ereport`: Reports error for invalid binary representation
  - `PG_RETURN_LINE_P`: Returns LINE pointer as Datum
- Called from (representative examples):
  - PostgreSQL binary protocol system (no direct function references found)

## Notes and Other Information
- Part of PostgreSQL's geometric data type system in `src/backend/utils/adt/geo_ops.c`
- Counterpart to `line_send` for binary serialization/deserialization
- Performs validation to ensure mathematical validity of line coefficients
- Uses standard PostgreSQL error reporting for invalid binary data
- Memory allocation managed by PostgreSQL's memory context system
- Line numbers: 1038-1060 in geo_ops.c