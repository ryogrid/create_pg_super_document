# gist_point_fetch

## Location
[src/backend/access/gist/gistproc.c:1196-1215](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistproc.c#L1196-L1215)

## Overview
Implements the GiST fetch method for points, converting compressed bounding box representation back to the original point format for query result presentation.

## Definition
```c
Datum gist_point_fetch(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the fetch method for point data types in GiST indexes, performing the inverse operation of gist_point_compress. The fetch method is called when the index needs to return the original data format to the query executor. Since points are compressed into degenerate bounding boxes during index operations (where box->high equals box->low), this function extracts the point coordinates from the bounding box format.

The function creates a new Point structure and populates it with coordinates from the bounding box's high point (which is identical to the low point for compressed points). This restoration is essential for maintaining data type consistency when returning query results to upper-level PostgreSQL components that expect Point data rather than BOX data.

## Parameters / Member Variables
- `entry`: GiST entry pointer containing the compressed bounding box representation of a point

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md): Memory allocation for creating new point and entry structures
  - [DatumGetBoxP](../D/DatumGetBoxP.md): Extracts bounding box data from the entry's key
  - `gistentryinit`: Initializes a new GiST entry with the fetched point data
  - [PointerGetDatum](../P/PointerGetDatum.md): Converts point pointer to Datum for storage
  - [GISTENTRY](../G/GISTENTRY.md): GiST entry structure type
  - [BOX](../B/BOX.md): Bounding box structure type
  - [Point](../P/Point.md): Point coordinate structure type
- Called from (representative examples):
  - Referenced by GiST access method during index scan operations (no direct references found in codebase)

## Notes and Other Information
- Performs the inverse operation of gist_point_compress
- Extracts point coordinates from degenerate bounding boxes using the high coordinates (equivalent to low coordinates)
- Essential for maintaining data type consistency between internal index representation and external query interface
- Allocates new memory for both the point and return entry structures
- Part of the standard GiST operator class implementation for geometric point types
- Ensures that point queries return Point data types rather than the internal BOX representation used within the index

## Simplified Source

```c
Datum gist_point_fetch(PG_FUNCTION_ARGS) {
    GISTENTRY *entry = (GISTENTRY *) PG_GETARG_POINTER(0);
    BOX *boundbox = DatumGetBoxP(entry->key);
    Point *point;
    GISTENTRY *retval;

    retval = palloc(sizeof(GISTENTRY));

    // Extract point coordinates from degenerate bounding box
    point = (Point *) palloc(sizeof(Point));
    point->x = boundbox->high.x;
    point->y = boundbox->high.y;

    // Create new entry with original point format
    gistentryinit(*retval, PointerGetDatum(point),
                  entry->rel, entry->page, entry->offset, false);

    PG_RETURN_POINTER(retval);
}
```