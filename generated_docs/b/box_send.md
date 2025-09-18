# box_send

## Location
src/backend/utils/adt/geo_ops.c: 501 - 517

## Overview
Converts a PostgreSQL BOX data type to its binary representation for transmission over the network protocol.

## Definition


## Detailed Description
The  function is part of PostgreSQL's binary input/output system for the BOX geometric data type. It serializes a BOX structure into a binary format suitable for network transmission. The function extracts the four coordinate values (high.x, high.y, low.x, low.y) from the BOX structure and writes them sequentially as 8-byte floating-point values using the PostgreSQL binary protocol functions.

This function is typically called automatically by the PostgreSQL system when a BOX value needs to be sent to a client application using the binary protocol, rather than the text protocol.

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention that provides access to function arguments
  - Argument 0: BOX pointer - the box structure to be serialized

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOX_P (retrieves BOX argument)
  - [pq_begintypsend](../p/pq_begintypsend.md) (initializes binary output buffer)
  - [pq_sendfloat8](../p/pq_sendfloat8.md) (sends 8-byte float values)
  - [pq_endtypsend](../p/pq_endtypsend.md) (finalizes binary output)
  - PG_RETURN_BYTEA_P (returns binary data)
- Called from:
  - PostgreSQL's binary protocol system (automatically invoked)

## Notes and Other Information
- The coordinate order in binary format is: high.x, high.y, low.x, low.y
- This function is the binary counterpart to box_out (text output) and box_recv (binary input)
- The binary format is more efficient than text format for network transmission
- Part of PostgreSQL's geometric data type system located in src/backend/utils/adt/geo_ops.c