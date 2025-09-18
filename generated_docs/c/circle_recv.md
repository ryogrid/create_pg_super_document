# circle_recv

## Location
src/backend/utils/adt/geo_ops.c: 4703 - 4726

## Overview
Deserializes a CIRCLE from PostgreSQL's external binary format, converting network byte order data into the internal CIRCLE structure.

## Definition


## Detailed Description
The `circle_recv` function is the binary input conversion routine for PostgreSQL's CIRCLE geometric type. It reads a binary representation of a circle from a StringInfo buffer and converts it into the internal CIRCLE data structure. This function is used during binary protocol communications, such as when data is received from network clients using the binary wire format or when reading from binary-format dumps.

The function reads three consecutive float8 values from the input buffer: the x and y coordinates of the center point, followed by the radius. It performs validation to ensure the radius is non-negative (though NaN values are accepted for special cases). Unlike text parsing, binary deserialization is straightforward as the data layout is predetermined and fixed.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments, containing:
  - StringInfo buffer with binary data (accessed via PG_GETARG_POINTER(0))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER (retrieves input buffer pointer)
  - [palloc](../p/palloc.md) (allocates memory for CIRCLE structure)
  - [pq_getmsgfloat8](../p/pq_getmsgfloat8.md) (reads float8 values from binary buffer)
  - ereport (reports validation errors)
  - PG_RETURN_CIRCLE_P (returns the deserialized circle)
- Types referenced:
  - StringInfo (input buffer type)
  - CIRCLE (output geometric type)
- Constants referenced:
  - ERROR (error level)
  - ERRCODE_INVALID_BINARY_REPRESENTATION (error code)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Counterpart to `circle_send` function for binary serialization/deserialization
- Used in PostgreSQL's binary wire protocol for efficient data transfer
- Accepts NaN values for coordinates and radius to handle special cases
- Validates that radius is non-negative, rejecting negative values with appropriate error messages
- More efficient than text parsing as it avoids string parsing overhead
- Reads data in network byte order as handled by pq_getmsgfloat8
- Part of PostgreSQL's type system for binary I/O operations
- Located in src/backend/utils/adt/geo_ops.c:4703-4726