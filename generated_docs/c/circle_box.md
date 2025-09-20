# circle_box

## Location
[src/backend/utils/adt/geo_ops.c:5186-5207](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L5186-L5207)

## Overview
Converts a circle to the largest square (box) that can be inscribed within it.

## Definition

```c
Datum
circle_box(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function performs a geometric conversion from a CIRCLE to a BOX (rectangular) data type. It calculates the largest square that can be inscribed within the given circle. The function computes the side length of this inscribed square using the formula: side = radius / √2. The resulting box is centered at the same point as the original circle, with corners positioned at equal distances from the center along both axes.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : Input CIRCLE structure accessed via 

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts CIRCLE argument from function args
  -  - PostgreSQL memory allocation function
  -  - PostgreSQL safe floating-point division
  -  - Square root mathematical function
  -  - PostgreSQL safe floating-point addition
  -  - PostgreSQL safe floating-point subtraction
  -  - Returns BOX result to PostgreSQL
- Data types used:
  -  - Input circle structure
  -  - Output box structure
  -  - Floating-point calculation type
  -  - PostgreSQL function return type
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The algorithm creates the largest square that fits inside the circle (inscribed square)
- The delta calculation uses radius/√2, which gives the distance from center to the edge of the inscribed square
- Uses PostgreSQL's safe arithmetic functions to prevent numerical errors
- The resulting box has equal width and height (making it a square)
- Can be called from SQL to convert circle geometric types to box types
- Memory allocation is handled by PostgreSQL's memory context system