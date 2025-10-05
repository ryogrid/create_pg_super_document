# circle_poly

## Location
[src/backend/utils/adt/geo_ops.c:5225-5284](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L5225-L5284)

## Overview
Converts a circle to a polygon with a specified number of vertices by approximating the circle's circumference with straight line segments.

## Definition

```c
Datum
circle_poly(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function creates a polygon approximation of a circle by generating vertices at regular angular intervals around the circle's circumference. The function takes the number of desired vertices and a circle as input, then calculates vertex positions using trigonometric functions. Each vertex is positioned at equal angular steps around the circle, starting from angle 0 and incrementing by 2π/npts for each subsequent vertex.

The function includes several validation checks: it ensures the circle has a non-zero radius (as a zero-radius circle cannot be meaningfully converted to a polygon), requires at least 2 vertices for a valid polygon, and checks for integer overflow when allocating memory for the polygon structure.

## Parameters / Member Variables
-  (int32): The number of vertices/points to generate for the polygon approximation (must be >= 2)
-  (CIRCLE*): Pointer to the input circle structure containing center coordinates and radius

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32, PG_GETARG_CIRCLE_P (parameter extraction macros)
  - FPzero (floating-point zero check)
  - ereport, errcode, errmsg (error reporting)
  - [palloc0](../p/palloc0.md) (memory allocation)
  - SET_VARSIZE (set PostgreSQL variable-length structure size)
  - [float8_div](../f/float8_div.md), float8_mul, float8_mi, float8_pl (floating-point arithmetic functions)
  - cos, sin (trigonometric functions)
  - [make_bound_box](../m/make_bound_box.md) (calculate bounding box for polygon)
  - PG_RETURN_POLYGON_P (return macro)
- Data types referenced:
  - CIRCLE, POLYGON (geometric data structures)
  - M_PI (mathematical constant)

## Notes and Other Information
- The function generates vertices starting from angle 0 and proceeding counter-clockwise around the circle
- Memory allocation includes overflow protection to prevent crashes with extremely large vertex counts
- The resulting polygon includes a properly calculated bounding box for efficient geometric operations
- Error handling covers invalid parameters (zero radius, insufficient vertices) and resource limits (excessive vertex count)
- The polygon vertices are calculated using the parametric circle equation: x = center.x - radius*cos(angle), y = center.y + radius*sin(angle)

## Simplified Source

```c
Datum circle_poly(PG_FUNCTION_ARGS) {
    int32 npts = PG_GETARG_INT32(0);
    CIRCLE *circle = PG_GETARG_CIRCLE_P(1);

    // Validate inputs
    if (FPzero(circle->radius))
        ereport(ERROR, (errmsg("cannot convert circle with radius zero to polygon")));
    if (npts < 2)
        ereport(ERROR, (errmsg("must request at least 2 points")));

    // Allocate polygon with overflow check
    int size = offsetof(POLYGON, p) + sizeof(Point) * npts;
    POLYGON *poly = (POLYGON *) palloc0(size);
    SET_VARSIZE(poly, size);
    poly->npts = npts;

    // Generate vertices at equal angular intervals
    float8 anglestep = 2.0 * M_PI / npts;
    for (int i = 0; i < npts; i++) {
        float8 angle = anglestep * i;
        poly->p[i].x = circle->center.x - circle->radius * cos(angle);
        poly->p[i].y = circle->center.y + circle->radius * sin(angle);
    }

    make_bound_box(poly);
    PG_RETURN_POLYGON_P(poly);
}
```