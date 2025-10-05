# poly_contained

## Location
[src/backend/utils/adt/geo_ops.c:3988-4007](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L3988-L4007)

## Overview
PostgreSQL function that determines if polygon A is contained within polygon B by switching the arguments and delegating to poly_contain_poly.

## Definition

```c
Datum
poly_contained(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the containment test for two polygons where the first polygon (polya) is tested to see if it is contained within the second polygon (polyb). The implementation is straightforward - it extracts both polygon arguments, switches their order, and calls the existing poly_contain_poly function to perform the actual containment logic. This approach leverages the mathematical relationship where "A contained in B" is equivalent to "B contains A". The function includes proper memory management for toasted (compressed) polygon inputs to prevent memory leaks, which is essential for rtree index operations.

## Parameters / Member Variables
- : PostgreSQL function call context containing two POLYGON arguments
  - Argument 0: The polygon to test for containment (polya)
  - Argument 1: The containing polygon (polyb)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POLYGON_P (extract polygon arguments)
  - [poly_contain_poly](poly_contain_poly.md) (perform the actual containment test)
  - PG_FREE_IF_COPY (memory cleanup for toasted inputs)
  - PG_RETURN_BOOL (return boolean result)
- Called from (representative examples):
  - No direct references found in the codebase (likely used via SQL operator @)

## Notes and Other Information
- The function is part of PostgreSQL's geometric data type operations in src/backend/utils/adt/geo_ops.c
- Memory management is explicitly handled for toasted inputs to prevent leaks during rtree index operations
- The containment test is implemented by argument reversal, demonstrating the symmetric relationship between containment operations
- Located at src/backend/utils/adt/geo_ops.c:3988-4007

## Simplified Source

```c
Datum poly_contained(PG_FUNCTION_ARGS) {
    POLYGON *polya = PG_GETARG_POLYGON_P(0);  // Polygon to test for containment
    POLYGON *polyb = PG_GETARG_POLYGON_P(1);  // Containing polygon
    bool result;

    // Reverse arguments: "A contained in B" = "B contains A"
    result = poly_contain_poly(polyb, polya);

    // Clean up toasted inputs for memory efficiency
    PG_FREE_IF_COPY(polya, 0);
    PG_FREE_IF_COPY(polyb, 1);

    PG_RETURN_BOOL(result);
}
```