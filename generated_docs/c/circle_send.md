# circle_send

## Location
src/backend/utils/adt/geo_ops.c: 4727 - 4750

## Overview
Serializes a CIRCLE structure into PostgreSQL's external binary format for efficient network transmission and storage.

## Definition


## Detailed Description
The `circle_send` function is the binary output conversion routine for PostgreSQL's CIRCLE geometric type. It takes a CIRCLE structure from the internal format and serializes it into a binary representation suitable for transmission over the network or storage in binary format. This function is used during binary protocol communications, such as when sending data to network clients using the binary wire format or when writing to binary-format dumps.

The function uses PostgreSQL's pq_send* family of functions to efficiently serialize the circle data. It writes three consecutive float8 values: the x and y coordinates of the center point, followed by the radius. The output is properly formatted for network transmission with appropriate byte ordering handled by the underlying pq_send functions.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments, containing:
  - Input CIRCLE structure (accessed via PG_GETARG_CIRCLE_P(0))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CIRCLE_P (retrieves input circle argument)
  - [pq_begintypsend](../p/pq_begintypsend.md) (initializes binary output buffer)
  - [pq_sendfloat8](../p/pq_sendfloat8.md) (writes float8 values to binary buffer)
  - [pq_endtypsend](../p/pq_endtypsend.md) (finalizes binary output buffer)
  - PG_RETURN_BYTEA_P (returns the serialized binary data)
- Types referenced:
  - CIRCLE (input geometric type)
  - [StringInfoData](../S/StringInfoData.md) (binary output buffer)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Counterpart to `circle_recv` function for binary serialization/deserialization
- Used in PostgreSQL's binary wire protocol for efficient data transfer
- Handles all valid CIRCLE values including those with NaN coordinates or radius
- More efficient than text output as it avoids string formatting overhead
- Writes data in network byte order as handled by pq_sendfloat8
- The output format is platform-independent and suitable for cross-system communication
- Part of PostgreSQL's type system for binary I/O operations
- The binary format consists of exactly 24 bytes (3 × 8-byte float8 values)
- Located in src/backend/utils/adt/geo_ops.c:4727-4750