# poly_recv

## Location
src/backend/utils/adt/geo_ops.c: 3475 - 3509

## Overview
The `poly_recv` function deserializes a POLYGON from PostgreSQLs binary external format, converting binary data back into the internal POLYGON data structure.

## Definition
```c
Datum poly_recv(PG_FUNCTION_ARGS)
```

## Detailed Description
This is a PostgreSQL binary input function that handles deserialization from the binary wire format to internal POLYGON representation. The binary format consists of an int32 point count followed by the coordinate data for each point as float8 pairs (x,y coordinates).

The function includes robust validation to prevent integer overflow attacks and malformed binary data. It explicitly validates the point count to ensure its positive and doesnt exceed memory allocation limits. After reading all coordinate data, it recomputes the bounding box rather than trusting any transmitted bounding box data, ensuring data integrity and consistency.

This approach prioritizes security and correctness over performance, as validating a transmitted bounding box would take similar time to recomputing it.

## Parameters / Member Variables
- Standard PostgreSQL function arguments accessed via:
  - `PG_GETARG_POINTER(0)`: StringInfo buffer containing the binary polygon data

## Dependencies
- Functions called/Symbols referenced:
  - `[pq_getmsgint](pq_getmsgint.md)`: Reads int32 from binary message buffer
  - `[pq_getmsgfloat8](pq_getmsgfloat8.md)`: Reads float8 from binary message buffer  
  - `ereport`: PostgreSQL error reporting for invalid data
  - `[palloc0](palloc0.md)`: PostgreSQL memory allocation with zero initialization
  - `SET_VARSIZE`: Sets the variable-length structure size
  - `[make_bound_box](../m/make_bound_box.md)`: Calculates the polygons bounding box
  - `PG_RETURN_POLYGON_P`: Returns the polygon result
- Called from (representative examples):
  - This is a PostgreSQL type receive function, typically called by the binary protocol handler and COPY BINARY operations

## Notes and Other Information
- This is a PostgreSQL type receive function registered in the system catalogs
- Used for binary protocol communication and COPY BINARY operations
- Includes comprehensive overflow protection and validation
- Recomputes bounding box for security rather than trusting transmitted data
- Works in conjunction with `poly_send` for binary serialization round-trips
- Part of PostgreSQLs binary data interchange system
- Prioritizes data integrity and security over performance optimization
- Handles variable-length binary polygon data safely