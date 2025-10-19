# circle_ne

## Location
[src/backend/utils/adt/geo_ops.c:4912-4920](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4912-L4920)

## Overview
Tests whether two circles have unequal areas within PostgreSQL's floating-point accuracy constraints.

## Definition
```c
Datum circle_ne(PG_FUNCTION_ARGS)
```

## Detailed Description
The `circle_ne` function compares two circles for area inequality. It computes the area of each circle using `circle_ar` and performs a floating-point inequality comparison using `FPne`. This function is the complement of `circle_eq`, returning true when the circles have different areas within PostgreSQL's floating-point accuracy tolerance.

## Parameters / Member Variables
- `circle1`: First circle argument obtained via `PG_GETARG_CIRCLE_P(0)`
- `circle2`: Second circle argument obtained via `PG_GETARG_CIRCLE_P(1)`

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_CIRCLE_P`: Macro to extract CIRCLE argument from function call
  - [circle_ar](circle_ar.md): Function to calculate the area of a circle
  - [FPne](../F/FPne.md): Floating-point inequality comparison with accuracy tolerance
  - `PG_RETURN_BOOL`: Macro to return boolean result
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function implements the inequality operator for circle types in PostgreSQL
- Uses area-based comparison rather than direct geometric property comparison
- Employs floating-point accuracy constraints via `FPne` for reliable inequality testing
- Located in `src/backend/utils/adt/geo_ops.c:4912-4920`
- Complement function to `circle_eq`
- Part of PostgreSQL's geometric data type comparison operators

## Simplified Source

```c
Datum
circle_ne(PG_FUNCTION_ARGS)
{
    CIRCLE *circle1 = PG_GETARG_CIRCLE_P(0);
    CIRCLE *circle2 = PG_GETARG_CIRCLE_P(1);

    // Compare circles by area inequality within floating-point accuracy
    PG_RETURN_BOOL(FPne(circle_ar(circle1), circle_ar(circle2)));
}
```