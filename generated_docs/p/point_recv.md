# point_recv

## Location
[src/backend/utils/adt/geo_ops.c:1853-1867](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1853-L1867)

## Overview
Converts external binary format data into PostgreSQL's internal Point data structure for network communication.

## Definition
```c
Datum point_recv(PG_FUNCTION_ARGS)
```

## Detailed Description
The `point_recv` function is responsible for deserializing Point data from PostgreSQL's binary wire protocol format. This function is used when receiving Point data over network connections in binary mode, such as from client applications using the binary protocol or during replication. It reads two consecutive float8 (double precision) values from the input buffer representing the x and y coordinates, and constructs a new Point structure.

## Parameters / Member Variables
- `buf`: StringInfo buffer containing binary data to be deserialized into a Point

## Dependencies
- Functions called/Symbols referenced:
  - [Point](../P/Point.md) - PostgreSQL's 2D point data structure  
  - [pq_getmsgfloat8](pq_getmsgfloat8.md) - Function to read float8 values from binary message buffer
  - `PG_RETURN_POINT_P` - Macro for returning Point data from PostgreSQL functions
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's type system)

## Notes and Other Information
- This function is part of PostgreSQL's binary I/O system for geometric data types
- Binary receive function for the Point data type, registered in the PostgreSQL type system catalog
- Complements `point_send` for binary serialization/deserialization
- Used in binary protocol communications for better performance compared to text format
- The binary format stores coordinates as IEEE 754 double precision floating point values
- Memory allocation uses `palloc()` which integrates with PostgreSQL's memory management

## Simplified Source
```c
Datum point_recv(PG_FUNCTION_ARGS) {
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
    Point *point = (Point *) palloc(sizeof(Point));

    // Read x and y coordinates from binary buffer
    point->x = pq_getmsgfloat8(buf);
    point->y = pq_getmsgfloat8(buf);

    PG_RETURN_POINT_P(point);
}
```