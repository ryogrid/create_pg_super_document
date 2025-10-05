# lseg_send

## Location
[src/backend/utils/adt/geo_ops.c:2111-2128](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2111-L2128)

## Overview
Converts an internal line segment (LSEG) representation to PostgreSQL's binary wire protocol format for transmission to clients.

## Definition
```c
Datum lseg_send(PG_FUNCTION_ARGS)
```

## Detailed Description
The `lseg_send` function is a PostgreSQL binary output function that serializes a line segment into PostgreSQL's binary wire protocol format. It extracts the LSEG parameter from function arguments and writes the four coordinate values (x1, y1, x2, y2) as float8 values to a binary buffer. The function follows PostgreSQL's standard binary serialization pattern: initializing a buffer, writing data sequentially, and returning the completed byte array. This enables efficient network transmission of geometric data types in binary format, which is more compact and faster to process than text format.

## Parameters / Member Variables
- Parameter 0: LSEG pointer - the line segment to be converted to binary format

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_LSEG_P` - macro to extract LSEG argument
  - [pq_begintypsend](../p/pq_begintypsend.md) - initializes binary output buffer
  - [pq_sendfloat8](../p/pq_sendfloat8.md) - writes a float8 value to the buffer (called 4 times for x1,y1,x2,y2)
  - [pq_endtypsend](../p/pq_endtypsend.md) - finalizes binary output buffer and returns bytea
  - `PG_RETURN_BYTEA_P` - macro to return bytea result
- Data types used:
  - [LSEG](../L/LSEG.md) - line segment data type
  - [StringInfoData](../S/StringInfoData.md) - buffer type for binary message construction

## Notes and Other Information
- This is a standard PostgreSQL binary send function, following the convention of `[typename]_send`
- Coordinates are written in order: point1.x, point1.y, point2.x, point2.y
- Part of the binary protocol infrastructure enabling efficient data transfer
- The binary format is platform-independent and follows PostgreSQL's standard wire protocol
- More efficient than text-based output for network communication and bulk data operations

## Simplified Source

```c
Datum
lseg_send(PG_FUNCTION_ARGS)
{
    LSEG *ls = PG_GETARG_LSEG_P(0);
    StringInfoData buf;

    // Initialize binary output buffer
    pq_begintypsend(&buf);

    // Write coordinates to buffer: x1, y1, x2, y2
    pq_sendfloat8(&buf, ls->p[0].x);  // First point x
    pq_sendfloat8(&buf, ls->p[0].y);  // First point y
    pq_sendfloat8(&buf, ls->p[1].x);  // Second point x
    pq_sendfloat8(&buf, ls->p[1].y);  // Second point y

    // Finalize buffer and return as bytea
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}
```