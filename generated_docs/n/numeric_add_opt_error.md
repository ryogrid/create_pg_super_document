# numeric_add_opt_error

## Location
src/backend/utils/adt/numeric.c: 2883 - 2940

## Overview
Internal implementation of numeric addition with optional error handling, providing the core arbitrary precision arithmetic logic for PostgreSQL's numeric addition operations.

## Definition


## Detailed Description
This function performs the actual addition of two PostgreSQL numeric values, handling all the complexity of arbitrary precision decimal arithmetic. Unlike numeric_add, this function provides optional error handling through the have_error parameter, allowing callers to handle arithmetic errors programmatically rather than through PostgreSQL's standard exception mechanism.

The function implements comprehensive special value handling for NaN and infinity cases according to IEEE-like semantics, then delegates finite arithmetic to the high-precision add_var function. It properly handles edge cases like Inf + (-Inf) = NaN while maintaining mathematical correctness for all other combinations.

## Parameters / Member Variables
- : The first numeric operand to add
- : The second numeric operand to add
- : Optional pointer to boolean flag; if provided and an error occurs, this is set to true and NULL is returned instead of raising an exception

## Dependencies
- Functions called/Symbols referenced:
  - NUMERIC_IS_SPECIAL (special value detection)
  - NUMERIC_IS_NAN (NaN detection)
  - NUMERIC_IS_PINF/NUMERIC_IS_NINF (infinity detection)
  - make_result (numeric result construction)
  - init_var_from_num (numeric variable initialization)
  - init_var (variable initialization)
  - add_var (core addition arithmetic)
  - make_result_opt_error (result construction with error handling)
  - free_var (memory cleanup)
- Called from (representative examples):
  - numeric_add (standard addition wrapper)
  - executeItemOptUnwrapTarget (JSON path execution)
  - timestamp_part_common (timestamp arithmetic)
  - timestamptz_part_common (timestamptz arithmetic)
  - interval_part_common (interval arithmetic)

## Notes and Other Information
- Core implementation function for all numeric addition operations in PostgreSQL
- Handles special IEEE-like arithmetic rules: NaN propagation, infinity arithmetic
- Inf + (-Inf) and (-Inf) + Inf both result in NaN as per mathematical convention
- Uses high-precision NumericVar arithmetic for exact decimal calculations
- Optional error handling allows integration with contexts that need custom error processing
- Memory management follows PostgreSQL conventions with proper variable cleanup
- Critical component of PostgreSQL's arbitrary precision numeric system
- Used both directly and indirectly throughout the database system for precise arithmetic