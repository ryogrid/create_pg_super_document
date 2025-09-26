# PATH_CLOSED

## Location
[src/backend/utils/adt/geo_ops.c:75-156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L75-L156)

## Overview
PATH_CLOSED is an enumeration constant that represents a closed geometric path in PostgreSQL's geometric data type system.

## Definition

```c
struct(Point *result, float8 x, float8 y);
```
## Detailed Description
PATH_CLOSED is one of three possible values in the  enumeration, used to specify the delimiter type when encoding geometric paths for output. It indicates that a geometric path is closed, meaning the path forms a loop where the last point connects back to the first point. When a path is marked as closed, it will be rendered with parentheses '()' as delimiters in its string representation.

The enumeration is used primarily in path encoding and output functions to determine how to format the textual representation of path data structures. A closed path represents a polygon-like structure where all points are connected in sequence, with an implicit connection from the last point back to the first.

## Parameters / Member Variables
This is an enumeration constant with no parameters or members.

## Dependencies
- Functions called/Symbols referenced:
  - Used as part of  definition
- Called from (representative examples):
  -  (src/backend/utils/adt/geo_ops.c:349, 371)
  -  (src/backend/utils/adt/geo_ops.c:1478)
  -  (src/backend/utils/adt/geo_ops.c:3463)

## Notes and Other Information
- PATH_CLOSED causes paths to be delimited with parentheses '(' and ')' characters (LDELIM and RDELIM)
- Used in conjunction with PATH_OPEN (delimited with '[' and ']') and PATH_NONE (no delimiters)
- The choice between PATH_CLOSED and PATH_OPEN depends on the  boolean field of the PATH data structure
- Part of PostgreSQL's geometric data type system for handling 2D geometric objects
- File location: src/backend/utils/adt/geo_ops.c:75