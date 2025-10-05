# lseg_out

## Location
[src/backend/utils/adt/geo_ops.c:2081-2091](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2081-L2091)

## Overview
Converts a line segment (LSEG) data type to its external string representation for output to the client.

## Definition

```c
Datum
lseg_out(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL output function that converts an internal line segment representation to a human-readable string format. It extracts the LSEG parameter from the function arguments and uses the  function to format it as an open path with 2 points, representing the start and end points of the line segment. The resulting string follows PostgreSQL's standard format for line segments: .

## Parameters / Member Variables
- Parameter 0: LSEG pointer - the line segment to be converted to string format

## Dependencies
- Functions called/Symbols referenced:
  -  - macro to extract LSEG argument
  -  - encodes path points into string representation
  -  - macro to return C string result
- Constants used:
  -  - indicates open path format for encoding
  -  - line segment data type

## Notes and Other Information
- This is a standard PostgreSQL type output function, following the convention of
- The function leverages existing path encoding functionality to format line segments
- Line segments are represented as open paths with exactly 2 points
- The output format is compatible with PostgreSQL's line segment input parser

## Simplified Source

```c
Datum
lseg_out(PG_FUNCTION_ARGS)
{
    // Get line segment from function arguments
    LSEG *ls = PG_GETARG_LSEG_P(0);

    // Convert to string format using path encoding: "[(x1,y1),(x2,y2)]"
    PG_RETURN_CSTRING(path_encode(PATH_OPEN, 2, &ls->p[0]));
}
```