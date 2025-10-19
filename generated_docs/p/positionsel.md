# positionsel

## Location
[src/backend/utils/adt/geo_selfuncs.c:67-72](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_selfuncs.c#L67-L72)

## Overview
A selectivity estimation function for geometric positional operators that determine relative positioning between geometric objects.

## Definition
```c
Datum positionsel(PG_FUNCTION_ARGS)
```

## Detailed Description
The `positionsel` function provides selectivity estimation for geometric operators that test positional relationships between geometric objects, specifically for operators that determine if one box is strictly positioned relative to another (left of, right of, above, or below).

According to the source comments, this function estimates "How likely is a box to be strictly left of (right of, above, below) a given box?". It returns a selectivity value of 0.1 (10%), which is notably higher than the area-based selectivity functions (`areasel` and `areajoinsel`) that return 0.005.

The higher selectivity value for positional operators reflects the intuition that positional relationships (being to the left, right, above, or below) are generally more common than complex geometric relationships like overlaps, especially when considering the typical distribution of geometric data in spatial applications.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - PG_RETURN_FLOAT8 (PostgreSQL macro for returning float8 values)
- Called from (representative examples):
  - Used by PostgreSQL's query optimizer for selectivity estimation of geometric positional operators (<<, >>, &<, &>, etc.)

## Notes and Other Information
- Returns a selectivity of 0.1 (10%), higher than area-based operators
- Specifically designed for strict positional relationships (left of, right of, above, below)
- Part of the geometric selectivity function family in geo_selfuncs.c
- The higher selectivity reflects the more common nature of positional relationships compared to overlap operations
- Still uses a hardcoded value due to the same fundamental challenge of unknown spatial data distribution

## Simplified Source

```c
Datum positionsel(PG_FUNCTION_ARGS) {
    // Return selectivity estimate for positional operators (left, right, above, below)
    PG_RETURN_FLOAT8(0.1);
}
```

This selectivity function estimates how likely one geometric object is to be positioned relative to another (left of, right of, above, below). It returns 10%, which is higher than area-based operators since positional relationships are generally more common than complex geometric overlaps.