# lseg_recv

## Location
src/backend/utils/adt/geo_ops.c: 2092 - 2110

## Overview
Converts external binary format data to an internal line segment (LSEG) representation for PostgreSQL binary protocol communication.

## Definition
```c
Datum lseg_recv(PG_FUNCTION_ARGS)
```

## Detailed Description
The `lseg_recv` function is a PostgreSQL binary input function that deserializes a line segment from PostgreSQL's binary wire protocol format. It reads four consecutive float8 values from the input buffer representing the x,y coordinates of two points that define the line segment. The function allocates memory for a new LSEG structure and populates it with the coordinate data received from the client. This function is part of PostgreSQL's binary I/O system, enabling efficient transmission of geometric data types over network connections.

## Parameters / Member Variables
- Function uses `PG_FUNCTION_ARGS` macro for parameter access
- Parameter 0: StringInfo buffer - contains the binary-encoded line segment data from the client

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_POINTER` - macro to extract StringInfo buffer argument
  - [palloc](../p/palloc.md) - PostgreSQL memory allocation function
  - [pq_getmsgfloat8](../p/pq_getmsgfloat8.md) - reads a float8 value from the message buffer (called 4 times for x1,y1,x2,y2)
  - `PG_RETURN_LSEG_P` - macro to return LSEG pointer result
- Data types used:
  - `StringInfo` - buffer type for binary message data
  - [LSEG](../L/LSEG.md) - line segment data type

## Notes and Other Information
- This is a standard PostgreSQL binary receive function, following the convention of `[typename]_recv`
- Coordinates are read in order: point1.x, point1.y, point2.x, point2.y
- Uses PostgreSQL's memory management system (palloc) for allocation
- Part of the binary protocol infrastructure enabling efficient data transfer
- The binary format is platform-independent and follows PostgreSQL's standard wire protocol