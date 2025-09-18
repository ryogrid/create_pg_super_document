# poly_center

## Location
src/backend/utils/adt/geo_ops.c: 4503 - 4518

## Overview
Computes and returns the center point of a polygon by converting the polygon to a circle and returning the circle's center.

## Definition
```c
Datum poly_center(PG_FUNCTION_ARGS)
```

## Detailed Description
The `poly_center` function calculates the center point of a polygon by utilizing the `poly_to_circle` conversion function. It first converts the input polygon to an equivalent circle representation, then extracts the center point of that circle as the result. This approach provides a consistent method for determining a polygon's center point based on the circle that best represents the polygon's shape and area.

## Parameters / Member Variables
- Input: A POLYGON pointer obtained via `PG_GETARG_POLYGON_P(0)` - the polygon whose center is to be computed
- Returns: A Point pointer via `PG_RETURN_POINT_P()` - the center point of the polygon

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POLYGON_P (macro to extract POLYGON argument)
  - palloc (memory allocation for result Point)
  - poly_to_circle (converts polygon to equivalent circle)
  - PG_RETURN_POINT_P (macro to return Point result)
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- The center calculation is based on the equivalent circle representation of the polygon
- Memory is allocated for the result Point structure using palloc()
- The function leverages existing polygon-to-circle conversion logic rather than implementing center calculation directly
- This method ensures consistency with other geometric operations that work with circles
- Located in src/backend/utils/adt/geo_ops.c:4503-4518