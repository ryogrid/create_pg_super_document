# point_send

## Location
src/backend/utils/adt/geo_ops.c: 1868 - 1883

## Overview
Converts PostgreSQL's internal Point data structure into binary format for network communication.

## Definition
```c
Datum point_send(PG_FUNCTION_ARGS)
```

## Detailed Description
The `point_send` function is responsible for serializing Point data into PostgreSQL's binary wire protocol format. This function is used when sending Point data over network connections in binary mode, such as to client applications using the binary protocol or during replication. It creates a binary buffer and writes the x and y coordinates as consecutive float8 (double precision) values, then returns the serialized data as a bytea.

## Parameters / Member Variables
- `pt`: Input Point structure containing x and y coordinates to be serialized

## Dependencies
- Functions called/Symbols referenced:
  - `Point` - PostgreSQL's 2D point data structure
  - `PG_GETARG_POINT_P` - Macro for extracting Point argument from PostgreSQL function call
  - `pq_begintypsend` - Initialize binary output buffer for type serialization
  - `pq_sendfloat8` - Write float8 value to binary output buffer  
  - `pq_endtypsend` - Finalize binary output buffer and return bytea
  - `PG_RETURN_BYTEA_P` - Macro for returning bytea data from PostgreSQL functions
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's type system)

## Notes and Other Information
- This function is part of PostgreSQL's binary I/O system for geometric data types
- Binary send function for the Point data type, registered in the PostgreSQL type system catalog
- Complements `point_recv` for binary serialization/deserialization
- Used in binary protocol communications for better performance compared to text format
- The binary format stores coordinates as IEEE 754 double precision floating point values
- Uses PostgreSQL's standard binary serialization infrastructure for consistent wire protocol format