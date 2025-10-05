# poly_contain_poly

## Location
[src/backend/utils/adt/geo_ops.c:3938-3965](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L3938-L3965)

## Overview
poly_contain_poly is a static function that determines whether the first polygon completely contains the second polygon.

## Definition
static bool poly_contain_poly(POLYGON *contains_poly, POLYGON *contained_poly)

## Detailed Description
poly_contain_poly implements a comprehensive polygon containment test by first performing a quick bounding box check for early rejection, then systematically verifying that every edge of the potentially contained polygon lies entirely within the containing polygon. The function uses an optimized two-phase approach: it first checks if the contained polygon's bounding box fits within the containing polygon's bounding box, and if so, proceeds to test each edge of the contained polygon using lseg_inside_poly. This approach ensures both efficiency (through early bounding box rejection) and accuracy (through complete edge-by-edge validation).

## Parameters / Member Variables
- : The polygon that potentially contains the other polygon
- : The polygon being tested for containment

## Dependencies
- Functions called/Symbols referenced:
  - [box_contain_box](../b/box_contain_box.md): Quick bounding box containment test for early rejection
  - [lseg_inside_poly](../l/lseg_inside_poly.md): Tests whether each edge of contained_poly lies inside contains_poly
- Called from (representative examples):
  - [poly_contain](poly_contain.md): PostgreSQL function wrapper for polygon containment operator
  - [poly_contained](poly_contained.md): PostgreSQL function wrapper for reverse containment test
  - [PATH_CLOSED](../P/PATH_CLOSED.md): For closed path containment testing

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:3938-3965
- Uses Assert to validate that both polygons have at least one vertex
- Implements a two-phase optimization strategy: bounding box check followed by edge-by-edge verification
- The algorithm iterates through all edges of the contained polygon, constructing line segments and testing each one for containment
- Returns false immediately if any edge of the contained polygon lies outside the containing polygon
- Critical component of PostgreSQL's geometric containment operators for polygon data types
- The bounding box check provides significant performance improvement by avoiding expensive edge testing when containment is impossible

## Simplified Source

```c
static bool poly_contain_poly(POLYGON *contains_poly, POLYGON *contained_poly) {
    int i;
    LSEG s;

    // Validate both polygons have vertices
    Assert(contains_poly->npts > 0 && contained_poly->npts > 0);

    // Quick bounding box containment check for early rejection
    if (!box_contain_box(&contains_poly->boundbox, &contained_poly->boundbox))
        return false;

    // Test each edge of contained polygon for full containment
    s.p[0] = contained_poly->p[contained_poly->npts - 1];

    for (i = 0; i < contained_poly->npts; i++) {
        s.p[1] = contained_poly->p[i];
        // If any edge is not inside containing polygon, return false
        if (!lseg_inside_poly(s.p, s.p + 1, contains_poly, 0))
            return false;
        s.p[0] = s.p[1];  // Move to next edge
    }

    return true;  // All edges contained
}
```