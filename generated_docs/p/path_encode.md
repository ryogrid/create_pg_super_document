# path_encode

## Location
[src/backend/utils/adt/geo_ops.c:340-391](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L340-L391)

## Overview
Encodes a geometric path data structure into its string representation, handling both open and closed path types with appropriate delimiters.

## Definition

```c
static char *
path_encode(enum path_delim path_delim, int npts, Point *pt)
```
## Detailed Description
The  function converts an array of Point structures into a properly formatted string representation of a geometric path. It handles three types of path delimiters: closed paths (surrounded by ), open paths (surrounded by ), and paths with no outer delimiters. Each point in the path is formatted as  with points separated by commas. The function uses PostgreSQL's StringInfo mechanism for efficient string building.

## Parameters / Member Variables
- `path_delim`: Enumeration value specifying the type of path delimiter (PATH_CLOSED, PATH_OPEN, or PATH_NONE)
- `npts`: Integer representing the number of points in the path
- `*pt`: Pointer to an array of Point structures containing the path coordinates
## Dependencies
- Functions called/Symbols referenced:
  - [initStringInfo](../i/initStringInfo.md)
  - [appendStringInfoChar](../a/appendStringInfoChar.md)
  - [pair_encode](pair_encode.md)
  - [Point](../P/Point.md) (struct)
  - path_delim (enum)
  - [PATH_CLOSED](../P/PATH_CLOSED.md), PATH_OPEN, PATH_NONE (enum values)
  - LDELIM, RDELIM, LDELIM_EP, RDELIM_EP, DELIM (delimiter constants)
- Called from (representative examples):
  - [box_out](../b/box_out.md)
  - [path_out](path_out.md)
  - [point_out](point_out.md)
  - [lseg_out](../l/lseg_out.md)
  - [poly_out](poly_out.md)

## Notes and Other Information
This is a static utility function used internally by various geometric output functions in PostgreSQL. The function handles three distinct formatting styles for paths depending on whether they represent closed polygons, open line segments, or raw coordinate sequences. The resulting string follows PostgreSQL's standard geometric data type output format.

## Simplified Source

```c
static char *path_encode(enum path_delim path_delim, int npts, Point *pt) {
    StringInfoData str;
    int i;

    initStringInfo(&str);

    // Add opening delimiter based on path type
    switch (path_delim) {
        case PATH_CLOSED:
            appendStringInfoChar(&str, LDELIM);    // '('
            break;
        case PATH_OPEN:
            appendStringInfoChar(&str, LDELIM_EP); // '['
            break;
        case PATH_NONE:
            break;
    }

    // Encode each point as (x,y) with comma separators
    for (i = 0; i < npts; i++) {
        if (i > 0)
            appendStringInfoChar(&str, DELIM);     // ','
        appendStringInfoChar(&str, LDELIM);        // '('
        pair_encode(pt->x, pt->y, &str);
        appendStringInfoChar(&str, RDELIM);        // ')'
        pt++;
    }

    // Add closing delimiter based on path type
    switch (path_delim) {
        case PATH_CLOSED:
            appendStringInfoChar(&str, RDELIM);    // ')'
            break;
        case PATH_OPEN:
            appendStringInfoChar(&str, RDELIM_EP); // ']'
            break;
        case PATH_NONE:
            break;
    }

    return str.data;
}
```