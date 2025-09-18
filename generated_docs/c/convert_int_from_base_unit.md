# convert_int_from_base_unit

## Location
src/backend/utils/misc/guc.c: 2731 - 2772

## Overview
convert_int_from_base_unit converts PostgreSQL internal base unit values back to human-friendly units for display, selecting the largest unit that represents the value without loss.

## Definition
static void convert_int_from_base_unit(int64 base_value, int base_unit, int64 *value, const char **unit)

## Detailed Description
This static function performs the reverse operation of convert_to_base_unit, converting internal base unit values back to human-readable units for display purposes. The function implements an intelligent unit selection algorithm that chooses the greatest unit that can represent the value exactly without fractional loss.

Key features include:
1. Selection of appropriate conversion table (memory or time) based on base_unit flags
2. Iteration through conversion table entries ordered from largest to smallest units
3. Selection of the first unit that divides the base value evenly (no remainder)
4. Fallback to base unit if no larger unit provides exact representation

For example, with memory units: 1024 KB converts to 1 MB, but 1025 KB remains as 1025 KB since MB conversion would lose precision.

## Parameters / Member Variables
- : The value in base units to convert from
- : Integer flag indicating the base unit type (contains GUC_UNIT_MEMORY for memory units)
- : Pointer to int64 where the converted value will be stored
- : Pointer to const char* where the selected unit string will be stored

## Dependencies
- Functions called/Symbols referenced:
  - rint: Round to nearest integer function
  - memory_unit_conversion_table: Global table for memory unit conversions
  - time_unit_conversion_table: Global table for time unit conversions
  - Assert: Debug assertion macro
- Called from (representative examples):
  - [ShowGUCOption](../S/ShowGUCOption.md): Display GUC option values in src/backend/utils/misc/guc.c:5507

## Notes and Other Information
- Static function, only accessible within guc.c
- Assumes conversion tables are ordered from greatest to smallest units
- Uses modulo operation to ensure exact conversion without loss
- Returns the first unit that provides exact division (no remainder)
- Essential for user-friendly display of configuration parameter values
- Always sets a valid unit (Assert ensures *unit != NULL)
- Handles both memory and time units through the same interface
- Part of PostgreSQL's bidirectional unit conversion system for configuration display