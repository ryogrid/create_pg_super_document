# path_recv

## Location
src/backend/utils/adt/geo_ops.c: 1488 - 1525

## Overview
Binary input function that converts external binary format to PostgreSQL's internal PATH data type representation.

## Definition


## Detailed Description
The  function is responsible for deserializing path data from PostgreSQL's binary format into the internal PATH structure. It reads a closed flag (boolean), the number of points (int32), and then the coordinate data for each point. The function performs validation to ensure the number of points is within valid bounds and allocates memory for the PATH structure with the appropriate size based on the number of points.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - : StringInfo pointer to the binary data buffer containing the serialized path

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgbyte](pq_getmsgbyte.md) (reads boolean closed flag)
  - [pq_getmsgint](pq_getmsgint.md) (reads int32 number of points)
  - [Point](../P/Point.md) (coordinate structure)
  - [PATH](../P/PATH.md) (path data type structure)
  - SET_VARSIZE (sets variable-length object size)
  - [pq_getmsgfloat8](pq_getmsgfloat8.md) (reads float8 coordinate values)
  - PG_RETURN_PATH_P (returns PATH pointer as Datum)
- Called from (representative examples):
  - No direct references found (likely referenced through function pointer tables)

## Notes and Other Information
- Validates number of points to prevent integer overflow in size calculation
- Uses  to calculate total structure size
- Sets unused pad bytes to zero for stability
- External binary format: closed flag (1 byte) + point count (4 bytes) + point coordinates (16 bytes per point)
- Throws ERROR with ERRCODE_INVALID_BINARY_REPRESENTATION for invalid point counts