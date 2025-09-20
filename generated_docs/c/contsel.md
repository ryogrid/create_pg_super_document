# contsel

## Location
[src/backend/utils/adt/geo_selfuncs.c:86-91](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_selfuncs.c#L86-L91)

## Overview
A selectivity estimation function for geometric containment operators that test whether one geometric object contains or is contained by another.

## Definition
```c
Datum contsel(PG_FUNCTION_ARGS)
```

## Detailed Description
The `contsel` function provides selectivity estimation for geometric operators that test containment relationships between geometric objects. According to the source comments, this function estimates "How likely is a box to contain (be contained by) a given box?".

This function returns a selectivity value of 0.001 (0.1%), which is the most conservative estimate among all the geometric selectivity functions. The lower selectivity reflects the nature of containment operations - they represent a "tighter constraint than 'overlap'", as noted in the source comments. Containment is inherently more restrictive than overlap because one object must be completely inside another, whereas overlap only requires partial intersection.

The very low selectivity value acknowledges that containment relationships are relatively rare in typical geometric datasets, especially when compared to other spatial relationships like positional comparisons or even overlaps.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - PG_RETURN_FLOAT8 (PostgreSQL macro for returning float8 values)
- Called from (representative examples):
  - Used by PostgreSQL's query optimizer for selectivity estimation of geometric containment operators (@>, <@, etc.)

## Notes and Other Information
- Returns the most conservative selectivity of 0.001 (0.1%) among geometric operators
- Represents containment relationships which are tighter constraints than overlap operations
- Part of the geometric selectivity function family in geo_selfuncs.c
- The very low selectivity reflects the rarity of complete containment relationships in typical spatial data
- Comparison with other geometric selectivities: containment (0.001) < area-based/overlap (0.005) < positional (0.1)