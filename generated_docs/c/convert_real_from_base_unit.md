# convert_real_from_base_unit

## Location
[src/backend/utils/misc/guc.c:2773-2815](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L2773-L2815)

## Overview
Converts a floating-point configuration value from its base unit to a human-friendly unit representation for display purposes.

## Definition

```c
static void
convert_real_from_base_unit(double base_value, int base_unit,
							double *value, const char **unit)
```
## Detailed Description
This function takes a floating-point value stored internally in PostgreSQL's base units (like bytes for memory or milliseconds for time) and converts it to a more readable unit format. The conversion logic selects the most appropriate unit that will display the value as close to an integer as possible, making configuration values more user-friendly when displayed.

The function uses conversion tables to find suitable target units. It prioritizes exact divisors but uses a tolerance mechanism (1e-8) to handle floating-point roundoff errors. If no exact match is found, it defaults to the smallest available unit.

The conversion tables are determined by the base_unit parameter - memory units use the memory_unit_conversion_table while time units use the time_unit_conversion_table.

## Parameters / Member Variables
- : The floating-point value in base units that needs to be converted
- : Flags indicating the type of unit (GUC_UNIT_MEMORY or time units)
- : Output parameter - pointer to store the converted numerical value
- : Output parameter - pointer to store the string representation of the target unit

## Dependencies
- Functions called/Symbols referenced:
  - unit_conversion (struct type)
  - GUC_UNIT_MEMORY (constant)
  - memory_unit_conversion_table (global table)
  - time_unit_conversion_table (global table)
- Called from (representative examples):
  - ShowGUCOption

## Notes and Other Information
- Uses a tolerance of 1e-8 to determine if a conversion results in a "clean" integer value for display
- The function always ensures *unit is set to a valid value (asserts this condition)
- Designed specifically for PostgreSQL's GUC (Grand Unified Configuration) system
- Part of the internal configuration display mechanism, not exposed to user code