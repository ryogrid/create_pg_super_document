# gist_point_distance

## Location
[src/backend/access/gist/gistproc.c:1455-1478](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistproc.c#L1455-L1478)

## Overview
Implements the GiST distance method for point data types, calculating the minimum distance between a query point and points stored in GiST index entries for nearest-neighbor search operations.

## Definition
```c
Datum gist_point_distance(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the GiST distance calculation method for point data types, primarily used in k-nearest neighbor (KNN) searches. It categorizes queries by strategy group and currently supports only point-to-point distance calculations. The function delegates the actual distance computation to the `computeDistance` utility function, which handles the differences between leaf and internal node entries.

For leaf entries, it calculates the exact distance between two points. For internal entries, it computes the minimum distance from the query point to the bounding box represented by the entry, enabling efficient pruning during nearest-neighbor searches.

## Parameters / Member Variables
- `entry`: GiST entry containing the indexed point data and associated bounding box information
- `strategy`: Strategy number indicating the type of distance operation (currently only point-to-point supported)
- Returns: Distance as a float8 value representing the minimum distance between the query point and the index entry

## Dependencies
- Functions called/Symbols referenced:
  - [computeDistance](../c/computeDistance.md)
  - `GIST_LEAF`
  - [DatumGetBoxP](../D/DatumGetBoxP.md)
  - `PG_GETARG_POINT_P`
  - `GeoStrategyNumberOffset`
  - `PointStrategyNumberGroup`
- Called from (representative examples):
  - GiST index access methods during KNN searches (indirectly through function pointers)

## Notes and Other Information
- Currently only supports `PointStrategyNumberGroup` queries; other strategy groups result in an error
- Uses the same `computeDistance` utility function as other geometric distance methods
- Essential for ORDER BY distance queries and KNN-GiST searches on point data
- Strategy number is divided by `GeoStrategyNumberOffset` to determine the query type category
- Returns 0.0 with compiler warning suppression when encountering unrecognized strategies