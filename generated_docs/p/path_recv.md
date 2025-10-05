# path_recv

## Location
[src/backend/utils/adt/geo_ops.c:1488-1525](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1488-L1525)

## Overview
Binary input function that converts external binary format to PostgreSQL's internal PATH data type representation.

## Definition

```c
Datum
path_recv(PG_FUNCTION_ARGS)
```
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

## Simplified Source

```c
Datum path_recv(PG_FUNCTION_ARGS) {
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
    PATH *path;
    int closed;
    int32 npts, i;
    int size;

    // Read binary format: closed flag + point count
    closed = pq_getmsgbyte(buf);
    npts = pq_getmsgint(buf, sizeof(int32));

    // Validate point count to prevent overflow
    if (npts <= 0 || npts >= (int32) ((INT_MAX - offsetof(PATH, p)) / sizeof(Point)))
        ereport(ERROR, (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                       errmsg("invalid number of points in external \"path\" value")));

    // Allocate PATH structure
    size = offsetof(PATH, p) + sizeof(path->p[0]) * npts;
    path = (PATH *) palloc(size);

    // Initialize PATH fields
    SET_VARSIZE(path, size);
    path->npts = npts;
    path->closed = (closed ? 1 : 0);
    path->dummy = 0;  // Prevent padding instability

    // Read coordinate data for each point
    for (i = 0; i < npts; i++) {
        path->p[i].x = pq_getmsgfloat8(buf);
        path->p[i].y = pq_getmsgfloat8(buf);
    }

    PG_RETURN_PATH_P(path);
}
```