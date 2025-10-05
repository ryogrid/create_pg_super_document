# poly_contain

## Location
[src/backend/utils/adt/geo_ops.c:3966-3987](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L3966-L3987)

## Overview
poly_contain is a PostgreSQL function that implements the containment operator to test whether the first polygon contains the second polygon.

## Definition
Datum poly_contain(PG_FUNCTION_ARGS)

## Detailed Description
poly_contain serves as the PostgreSQL function interface for the polygon containment operator (@>). It extracts two POLYGON arguments from the function call parameters, delegates the actual containment testing to poly_contain_poly(), and handles PostgreSQL-specific memory management for potentially toasted input values. The function follows PostgreSQL's standard function calling convention and returns a boolean result indicating whether the first polygon completely contains the second polygon. This function is essential for spatial queries and geometric operations involving polygon containment relationships.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS which provides access to:
  - First polygon (polya): The polygon that potentially contains the second polygon
  - Second polygon (polyb): The polygon being tested for containment

## Dependencies
- Functions called/Symbols referenced:
  - [poly_contain_poly](poly_contain_poly.md): Core polygon containment algorithm
  - PG_GETARG_POLYGON_P: Extracts POLYGON argument from function arguments
  - PG_FREE_IF_COPY: Manages memory for potentially toasted inputs
  - PG_RETURN_BOOL: Returns boolean result following PostgreSQL conventions
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:3966-3987
- Implements the @> containment operator for polygon data types in SQL
- Includes memory management for toasted inputs, which is essential for rtree indexes
- Acts as a thin wrapper around poly_contain_poly() while handling PostgreSQL-specific argument extraction and memory management
- The function is designed to be called from SQL queries that use polygon containment operations
- Critical component of PostgreSQL's geometric operator suite for spatial database applications

## Simplified Source

```c
Datum poly_contain(PG_FUNCTION_ARGS) {
    POLYGON *polya = PG_GETARG_POLYGON_P(0);  // Containing polygon
    POLYGON *polyb = PG_GETARG_POLYGON_P(1);  // Potentially contained polygon
    bool result;

    // Delegate to core containment algorithm
    result = poly_contain_poly(polya, polyb);

    // Clean up toasted inputs for memory efficiency
    PG_FREE_IF_COPY(polya, 0);
    PG_FREE_IF_COPY(polyb, 1);

    PG_RETURN_BOOL(result);
}
```