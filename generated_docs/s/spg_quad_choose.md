# spg_quad_choose

## Location
[src/backend/access/spgist/spgquadtreeproc.c:115-145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgquadtreeproc.c#L115-L145)

## Overview
SP-GiST quadtree choose function that determines which child node a new point should be inserted into during index operations.

## Definition

```c
Datum
spg_quad_choose(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements the choose method for SP-GiST quadtree indexes. When inserting a new point into the index, this function determines which of the four quadrant child nodes the point should be routed to. It uses the node's centroid point (stored as the prefix) to determine the appropriate quadrant using the  function. The function handles the special case where all points in a node are identical (allTheSame) by indicating a match with the current node rather than descending further.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro, includes:
  - :  structure containing input data (point to insert, prefix, node state)
  - :  structure to populate with routing decision

## Dependencies
- Functions called/Symbols referenced:
  - ,  (structure types)
  -  (structure type)  
  -  (datum conversion function)
  -  (point to datum conversion function)
  -  (quadrant determination function)
  -  (result type constant)
  -  (return macro)
- Called from (representative examples):
  - SP-GiST index insertion operations via function pointer

## Notes and Other Information
- This is a PostgreSQL SP-GiST operator class method, registered during index creation
- Returns  result type indicating which child node to descend to
- Uses  value of  to map from 1-4 quadrants to 0-3 node indices
- Handles degenerate case where all points are identical with  flag
- The  field is always set to 0 since quadtree doesn't compress levels
-  passes the original point data through to the next level unchanged

## Simplified Source

```c
Datum spg_quad_choose(PG_FUNCTION_ARGS) {
    spgChooseIn *in = (spgChooseIn *) PG_GETARG_POINTER(0);
    spgChooseOut *out = (spgChooseOut *) PG_GETARG_POINTER(1);
    Point *inPoint = DatumGetPointP(in->datum);

    // Handle degenerate case: all points are identical
    if (in->allTheSame) {
        out->resultType = spgMatchNode;
        out->result.matchNode.levelAdd = 0;
        out->result.matchNode.restDatum = PointPGetDatum(inPoint);
        PG_RETURN_VOID();
    }

    // Normal case: determine quadrant based on centroid
    Point *centroid = DatumGetPointP(in->prefixDatum);

    // Route to appropriate quadrant (getQuadrant returns 1-4, convert to 0-3)
    out->resultType = spgMatchNode;
    out->result.matchNode.nodeN = getQuadrant(centroid, inPoint) - 1;
    out->result.matchNode.levelAdd = 0;
    out->result.matchNode.restDatum = PointPGetDatum(inPoint);

    PG_RETURN_VOID();
}
```