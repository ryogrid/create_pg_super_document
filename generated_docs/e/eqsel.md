# eqsel

## Location
src/backend/utils/adt/selfuncs.c: 228 - 236

## Overview
The eqsel function provides selectivity estimation for equality operators ("=") and other operators with comparable selectivity behavior across any PostgreSQL data types.

## Definition


## Detailed Description
The eqsel function serves as the primary entry point for selectivity estimation of equality operations in PostgreSQL's query planner. It acts as a wrapper function that delegates the actual computation to eqsel_internal. This function is designed to handle equality selectivity estimation for any data types, including cases where the left and right operand data types may differ.

The function also supports operators that are not strict equality but have comparable selectivity behavior, such as "~=" (geometric approximate-match operators). This flexibility makes it a versatile tool for the query planner's cost estimation process.

## Parameters / Member Variables
- Uses standard PostgreSQL function calling convention (PG_FUNCTION_ARGS)
- Parameters are accessed through the fcinfo structure containing operator arguments and context

## Dependencies
- Functions called/Symbols referenced:
  - eqsel_internal
- Called from (representative examples):
  - Used by PostgreSQL's query planner for selectivity estimation
  - Referenced in operator catalog entries for equality operators

## Notes and Other Information
- Located in src/backend/utils/adt/selfuncs.c:228-236
- Returns a float8 value representing the estimated selectivity
- The function passes 'false' as the second parameter to eqsel_internal, indicating standard equality processing
- Part of PostgreSQL's selectivity function framework used by the query optimizer for cost-based planning