# brin_minmax_multi_distance_interval

## Location
src/backend/access/brin/brin_minmax_multi.c: 2155 - 2190

## Overview
Computes the distance between two interval values expressed as a fractional number of days, used by BRIN minmax multi operator classes for PostgreSQL interval data types.

## Definition
```c
Datum brin_minmax_multi_distance_interval(PG_FUNCTION_ARGS)
```

## Detailed Description
This function calculates the numerical distance between two PostgreSQL interval values by converting them to a common unit (fractional days) and computing their difference. The calculation handles the complex structure of intervals which contain separate fields for months, days, and time (microseconds). For consistency with interval_cmp_internal, it assumes months have 30 days. The function is part of the BRIN minmax multi operator class infrastructure, enabling efficient indexing of interval columns by maintaining multiple min/max pairs per block range.

## Parameters / Member Variables
- `PG_GETARG_INTERVAL_P(0)`: Pointer to the first interval value (ia)
- `PG_GETARG_INTERVAL_P(1)`: Pointer to the second interval value (ib)
- Returns: `float8` representing the distance in fractional days between the intervals

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INTERVAL_P (macro for extracting interval pointer arguments)
  - PG_RETURN_FLOAT8 (macro for returning float8 result)
  - USECS_PER_DAY (constant for microseconds per day conversion)
  - INT64CONST (macro for 64-bit integer constants)
  - Interval (PostgreSQL interval data type structure)
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- The function assumes the second interval (ib) >= first interval (ia) and includes an Assert to verify this
- Month calculations assume 30 days per month for consistency with PostgreSQL's interval comparison logic
- The calculation separates day fractions (sub-day time components) from full days for precision
- Distance is computed as: (day_difference + fractional_day_difference)
- This function is typically registered in BRIN operator class definitions for interval columns
- The approximation of 30 days per month may result in slightly less efficient ranges but maintains consistency with interval ordering