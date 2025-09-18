# float4recv

## Location
src/backend/utils/adt/float.c: 332 - 342

## Overview
PostgreSQL function that converts binary data from external format (network byte order) to a float4 value for binary input/output operations.

## Definition


## Detailed Description
The float4recv function is part of PostgreSQL's binary I/O system for the float4 data type. It receives binary data in a standardized external format (typically from network communication or binary file storage) and converts it to PostgreSQL's internal float4 representation. This function is the counterpart to float4send and is used in binary protocol operations, COPY BINARY operations, and other scenarios where float4 values need to be transmitted or stored in binary format.

The function uses PostgreSQL's StringInfo buffer system to read the binary data and relies on pq_getmsgfloat4() to handle the actual binary-to-float conversion including any necessary byte order conversions.

## Parameters / Member Variables
- Function accepts PostgreSQL function arguments via PG_FUNCTION_ARGS macro
- : StringInfo buffer containing the binary data, extracted from the first function argument using PG_GETARG_POINTER(0)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER (argument extraction)
  - pq_getmsgfloat4 (binary format parsing)
  - PG_RETURN_FLOAT4 (return value macro)
- Called from (representative examples):
  - No direct references found (likely called via PostgreSQL's function call mechanism for binary I/O)

## Notes and Other Information
- Part of PostgreSQL's binary I/O infrastructure for efficient data transmission
- Handles network byte order conversion automatically through pq_getmsgfloat4()
- Used in binary protocol communications between client and server
- Complementary function to float4send for round-trip binary serialization
- Registered in PostgreSQL's system catalogs as the binary input function for float4 type