# compare_values

## Location
src/backend/access/brin/brin_minmax_multi.c: 896 - 920

## Overview
Compares two Datum values for sorting purposes using a user-provided comparison function and collation.

## Definition


## Detailed Description
This function serves as a generic comparison function for sorting Datum values, designed to be compatible with standard C library sorting functions like qsort. It provides a standardized interface for comparing PostgreSQL Datum values of any type that has a comparison operator.

The function performs two comparison operations to determine the ordering relationship:
1. Tests if the first value is less than the second value
2. If not, tests if the second value is less than the first value
3. If neither comparison is true, the values are considered equal

The comparison is performed using PostgreSQL's function call interface (FunctionCall2Coll), which allows it to work with any data type's comparison function while respecting collation rules for text-based comparisons.

## Parameters / Member Variables
- : Pointer to the first Datum value to compare
- : Pointer to the second Datum value to compare
- : Pointer to compare_context structure containing the comparison function and collation information

## Dependencies
- Functions called/Symbols referenced:
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md) (used for performing the actual value comparisons)
  - [DatumGetBool](../D/DatumGetBool.md) (to extract boolean results from comparison functions)
- Called from (representative examples):
  - [AssertCheckRanges](../A/AssertCheckRanges.md) (for validation purposes)
  - [range_deduplicate_values](../r/range_deduplicate_values.md) (for sorting during deduplication)
  - [range_contains_value](../r/range_contains_value.md) (for binary search operations)
  - [reduce_expanded_ranges](../r/reduce_expanded_ranges.md) (for sorting during range reduction)

## Notes and Other Information
- Returns standard comparison function values: -1 (a < b), 0 (a == b), 1 (a > b)
- Compatible with qsort and other standard C sorting functions
- Uses PostgreSQL's function call interface to support any comparable data type
- Handles collation-aware comparisons through the compare_context structure
- Provides type-agnostic comparison capability for BRIN index operations
- Used extensively throughout the BRIN minmax multi implementation for various sorting needs