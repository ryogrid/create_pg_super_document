# clamp_cardinality_to_long

## Location
[src/backend/optimizer/path/costsize.c:254-283](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L254-L283)

## Overview
Safely casts a Cardinality value (double) to a long integer while handling edge cases and preventing overflow.

## Definition
```c
long clamp_cardinality_to_long(Cardinality x)
```

## Detailed Description
This function converts PostgreSQL's Cardinality type (which is a double) to a long integer while ensuring the result is numerically sound and within valid ranges. The function is designed to handle several problematic cases:

1. **NaN values**: Replaced with LONG_MAX to ensure a valid result
2. **Zero/negative values**: Set to 0 for consistent behavior
3. **Overflow protection**: Uses careful comparison to avoid casting issues when long is 64-bit

A key insight in this function is handling the precision issues when long is 64-bit. LONG_MAX cannot be represented exactly as a double, and casting it to double and back may result in overflow due to rounding. The function avoids this by comparing against (double) LONG_MAX rather than performing round-trip conversions.

## Parameters / Member Variables
- `x`: Input cardinality value of type Cardinality (which is a double)

## Dependencies
- Functions called/Symbols referenced:
  - `Cardinality`: PostgreSQL typedef for double, used for cardinality estimates
  - `isnan()`: C standard library function to check for NaN values
  - `LONG_MAX`: C standard library constant for maximum long value

- Called from (representative examples):
  - [buildSubPlanHash](../b/buildSubPlanHash.md): Building hash tables for subplans
  - [create_setop_plan](create_setop_plan.md): Creating set operation plans
  - [create_recursiveunion_plan](create_recursiveunion_plan.md): Creating recursive union plans
  - [make_agg](../m/make_agg.md): Creating aggregation nodes

## Notes and Other Information
- Located in src/backend/optimizer/path/costsize.c:254-283
- Handles precision issues specific to 64-bit long integers
- Always returns non-negative values (0 or positive)
- Used primarily in plan creation where integer cardinalities are needed
- Part of the interface between floating-point estimates and integer plan parameters
- Critical for preventing numeric overflow in plan node creation