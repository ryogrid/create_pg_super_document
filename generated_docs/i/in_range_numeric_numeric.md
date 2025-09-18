# in_range_numeric_numeric

## Location
src/backend/utils/adt/numeric.c: 2578 - 2712

## Overview
Implements the in_range support function for numeric types in PostgreSQL window functions, determining whether a given numeric value falls within a specified range relative to a base value and offset.

## Definition


## Detailed Description
This function is used by window functions with RANGE clauses to determine if a value is within a specified numeric range from a base value. It handles the semantics of "val BETWEEN base - offset AND base + offset" (or similar comparisons) while properly dealing with special numeric values like NaN and infinity.

The function performs comprehensive validation and handling of edge cases:
- Rejects negative or NaN offsets as per SQL specification
- Implements proper NaN semantics (NaN sorts after all non-NaN values)
- Handles infinite values and offsets correctly
- Uses high-precision numeric arithmetic for finite value calculations

## Parameters / Member Variables
-  (PG_GETARG_NUMERIC(0)): The numeric value being tested for inclusion in the range
-  (PG_GETARG_NUMERIC(1)): The base numeric value from which the range is calculated
-  (PG_GETARG_NUMERIC(2)): The numeric offset that defines the range size
-  (PG_GETARG_BOOL(3)): Boolean indicating whether to subtract offset (true) or add it (false)
-  (PG_GETARG_BOOL(4)): Boolean indicating the comparison direction (≤ vs ≥)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC (parameter extraction)
  - PG_GETARG_BOOL (parameter extraction)
  - NUMERIC_IS_NAN (NaN detection)
  - NUMERIC_IS_NINF/NUMERIC_IS_PINF (infinity detection)
  - NUMERIC_SIGN (sign determination)
  - [init_var_from_num](init_var_from_num.md) (numeric variable initialization)
  - [add_var](../a/add_var.md)/sub_var (arithmetic operations)
  - [cmp_var](../c/cmp_var.md) (numeric comparison)
  - [free_var](../f/free_var.md) (memory cleanup)
- Called from (representative examples):
  - Window function range processing (not directly referenced in indexed code)

## Notes and Other Information
- This function is critical for implementing SQL window functions with RANGE clauses
- Special attention is paid to PostgreSQL's numeric NaN semantics where NaN > any finite value
- The function validates that offsets are non-negative and non-NaN as required by SQL standard
- Infinite offsets are handled specially to produce mathematically correct results
- Uses PostgreSQL's high-precision NumericVar arithmetic for accurate calculations
- Memory management follows PostgreSQL conventions with proper cleanup of temporary variables