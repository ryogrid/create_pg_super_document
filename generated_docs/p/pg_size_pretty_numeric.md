# pg_size_pretty_numeric

## Location
src/backend/utils/adt/dbsize.c: 672 - 712

## Overview
This PostgreSQL SQL function converts a numeric byte size value into a human-readable string format with appropriate units (bytes, kB, MB, GB, TB, PB).

## Definition
```c
Datum pg_size_pretty_numeric(PG_FUNCTION_ARGS)
```

## Detailed Description
The `pg_size_pretty_numeric` function formats a numeric size value (in bytes) into a human-readable string representation. It iterates through predefined size units (bytes, kB, MB, GB, TB, PB) to find the most appropriate unit for display. The function applies different rounding strategies based on the unit configuration:

1. For each unit, it checks if the size is below the unit's limit or if it's the largest available unit
2. If a unit supports half-rounding, it applies `numeric_half_rounded` to the size
3. The function calculates the appropriate divisor using bit-shifting operations based on unit boundaries
4. It uses `numeric_truncated_divide` to scale the size to the selected unit
5. Finally, it formats the result as a text string combining the scaled number and unit name

The function uses a sophisticated bit-shifting calculation to determine the divisor, taking into account whether adjacent units use rounding to ensure proper scaling between units.

## Parameters / Member Variables
- Function accepts one argument via `PG_GETARG_NUMERIC(0)`: The numeric size value in bytes to be formatted

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC: Extracts Numeric argument from function call
  - size_pretty_units: Array of size unit definitions
  - [numeric_is_less](../n/numeric_is_less.md): Compares two Numeric values
  - [numeric_absolute](../n/numeric_absolute.md): Gets absolute value of Numeric
  - [int64_to_numeric](../i/int64_to_numeric.md): Converts int64 to Numeric
  - [numeric_half_rounded](../n/numeric_half_rounded.md): Applies half-rounding to Numeric
  - [numeric_to_cstring](../n/numeric_to_cstring.md): Converts Numeric to C string
  - [numeric_truncated_divide](../n/numeric_truncated_divide.md): Performs truncated division
  - [psprintf](psprintf.md): PostgreSQL sprintf function
  - cstring_to_text: Converts C string to PostgreSQL text
  - PG_RETURN_TEXT_P: Returns text result from function
- Called from (representative examples):
  - No direct references found (likely called via SQL)

## Notes and Other Information
This function is designed to be called from SQL as `pg_size_pretty(numeric)`. It provides a more precise alternative to the int64-based `pg_size_pretty` function by working with arbitrary precision numeric values. The function handles very large size values that would overflow int64 representation. The sophisticated unit scaling algorithm ensures consistent and intuitive size representations across all supported units. Located in src/backend/utils/adt/dbsize.c:672-712.