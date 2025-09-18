# gist_box_distance

## Location
[src/backend/access/gist/gistproc.c:1500-1525](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistproc.c#L1500-L1525)

## Overview
Implements the GiST distance method for box data types, calculating the minimum distance between a query point and boxes stored in GiST index entries for nearest-neighbor search operations.

## Definition
```c
Datum gist_box_distance(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the GiST distance calculation method for box data types, used in k-nearest neighbor (KNN) searches. It acts as a simple wrapper around the `gist_bbox_distance` utility function, providing the standard PostgreSQL function interface expected by the GiST access method.

The function extracts the GiST entry, query datum, and strategy number from the function arguments, then delegates the actual distance calculation to `gist_bbox_distance`. This approach allows box distance calculations to reuse the common bounding box distance logic implemented in the utility function.

The commented-out parameters (`subtype` and `recheck`) indicate that this function follows the standard GiST distance method signature but doesn\t currently use these additional parameters.