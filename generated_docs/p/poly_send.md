# poly_send

## Location
src/backend/utils/adt/geo_ops.c: 3510 - 3532

## Overview
The `poly_send` function serializes a POLYGON into PostgreSQLs binary external format for efficient network transmission and storage.

## Definition
```c
Datum poly_send(PG_FUNCTION_ARGS)
```

## Detailed Description
This is a PostgreSQL binary output function that handles serialization from internal POLYGON representation to the binary wire format. The function creates a compact binary representation consisting of the point count as an int32 followed by each points x and y coordinates as float8 values.

The binary format is designed to be platform-independent and efficient for network transmission. The function uses PostgreSQLs standard binary serialization infrastructure to ensure proper byte ordering and format consistency across different architectures and PostgreSQL versions.

The implementation is straightforward and focuses on creating a minimal, efficient binary representation. Notably, it does not include the bounding box in the binary format since `poly_recv` recomputes it anyway, reducing the transmitted data size.

## Parameters / Member Variables
- Standard PostgreSQL function arguments accessed via:
  - `PG_GETARG_POLYGON_P(0)`: The input POLYGON structure to be serialized to binary format

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_POLYGON_P`: Macro to extract POLYGON argument from function call
  - `pq_begintypsend`: Initializes binary output buffer
  - `pq_sendint32`: Writes int32 to binary buffer
  - `pq_sendfloat8`: Writes float8 to binary buffer
  - `pq_endtypsend`: Finalizes binary buffer and returns bytea
  - `PG_RETURN_BYTEA_P`: Returns the binary data result
- Called from (representative examples):
  - This is a PostgreSQL type send function, typically called by the binary protocol handler and COPY BINARY operations

## Notes and Other Information
- This is a PostgreSQL type send function registered in the system catalogs
- Used for binary protocol communication and COPY BINARY operations
- Creates platform-independent binary representation
- Works in conjunction with `poly_recv` for binary serialization round-trips
- Does not transmit bounding box data, optimizing for minimal size
- Part of PostgreSQLs binary data interchange system
- Ensures efficient network transmission of polygon data
- Produces compact binary format suitable for high-performance applications