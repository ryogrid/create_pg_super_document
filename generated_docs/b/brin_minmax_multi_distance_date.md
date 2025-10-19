# brin_minmax_multi_distance_date

## Location
[src/backend/access/brin/brin_minmax_multi.c:2080-2098](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L2080-L2098)

## Overview
Computes the distance between two date values for BRIN minmax multi indexes using simple subtraction.

## Definition
```c
Datum brin_minmax_multi_distance_date(PG_FUNCTION_ARGS)
```

## Detailed Description
This function calculates the distance between two date range boundaries in BRIN minmax multi indexes. It performs a straightforward subtraction operation between two DateADT values, converting them to float8 for the calculation. Since PostgreSQL dates are stored internally as integers representing days since a reference date (January 1, 2000), the subtraction directly yields the number of days between the two dates.

The function is used internally by the BRIN minmax multi operator class to determine the distance metric for date data types, which helps in index optimization and range query processing.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: First date value (lower bound) as DateADT
  - Argument 1: Second date value (upper bound) as DateADT

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_DATEADT`: PostgreSQL macro to extract DateADT from function arguments
  - `DateADT`: PostgreSQL date abstract data type (integer representing days)
  - `PG_RETURN_FLOAT8`: PostgreSQL return float8 value macro
- Called from (representative examples):
  - No direct references found (likely referenced through function pointers in BRIN operator classes)

## Notes and Other Information
- Uses simple integer subtraction converted to float8 for date distance calculation
- DateADT values represent days since PostgreSQL epoch (January 1, 2000)
- Includes assertion checking to validate non-negative result
- Returns distance in days as a float8 value for consistency across BRIN distance functions
- More straightforward than other distance functions due to dates simple integer representation
- Part of the BRIN minmax multi access method implementation
- Located in src/backend/access/brin/brin_minmax_multi.c:2080-2098

## Simplified Source

```c
Datum brin_minmax_multi_distance_date(PG_FUNCTION_ARGS) {
    // Extract date arguments (stored as integers representing days)
    DateADT dateVal1 = PG_GETARG_DATEADT(0);
    DateADT dateVal2 = PG_GETARG_DATEADT(1);

    // Calculate difference in days and convert to float8
    float8 delta = (float8) dateVal2 - (float8) dateVal1;

    PG_RETURN_FLOAT8(delta);
}
```