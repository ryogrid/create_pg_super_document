# box_intersect

## Location
[src/backend/utils/adt/geo_ops.c:908-932](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L908-L932)

## Overview
Returns the overlapping portion of two boxes, or NULL if they do not intersect.

## Definition

```c
struct(result, &box->high, &box->low);
```
## Detailed Description
The  function is a PostgreSQL geometric operation that computes the intersection of two BOX objects. It first checks if the boxes overlap using the  function. If they don't overlap, it returns NULL. If they do overlap, it calculates the intersection box by taking the minimum of the high coordinates and maximum of the low coordinates for both x and y dimensions. The resulting box represents the overlapping rectangular area between the two input boxes.

## Parameters / Member Variables
- Uses  macro to access function arguments:
  - : First BOX object (argument 0)
  - : Second BOX object (argument 1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOX_P (macro to extract BOX arguments)
  - [BOX](../B/BOX.md) (geometric box data type)
  - [box_ov](box_ov.md) (function to check if boxes overlap)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation function)
  - [float8_min](../f/float8_min.md) (minimum of two float8 values)
  - [float8_max](../f/float8_max.md) (maximum of two float8 values)
  - PG_RETURN_BOX_P (macro to return BOX result)
  - PG_RETURN_NULL (macro to return NULL)
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- This is a public PostgreSQL function accessible via SQL as the intersection operator
- Uses PostgreSQL's function calling convention with PG_FUNCTION_ARGS
- Allocates memory for the result box using palloc, which is managed by PostgreSQL's memory context system
- The intersection algorithm ensures the result box has the correct geometric properties by taking appropriate min/max values
- Returns NULL when boxes don't intersect, following PostgreSQL's convention for geometric operations
- Part of the "Funky operations" section in the geometric operations module

## Simplified Source

```c
Datum box_intersect(PG_FUNCTION_ARGS) {
    // Extract the two box arguments
    BOX *box1 = PG_GETARG_BOX_P(0);
    BOX *box2 = PG_GETARG_BOX_P(1);

    // Check if boxes overlap; return NULL if they don't
    if (!box_ov(box1, box2))
        return PG_RETURN_NULL();

    // Create result box with intersection coordinates
    BOX *result = (BOX *) palloc(sizeof(BOX));

    // Intersection bounds: min of highs, max of lows
    result->high.x = float8_min(box1->high.x, box2->high.x);
    result->low.x = float8_max(box1->low.x, box2->low.x);
    result->high.y = float8_min(box1->high.y, box2->high.y);
    result->low.y = float8_max(box1->low.y, box2->low.y);

    return PG_RETURN_BOX_P(result);
}
```