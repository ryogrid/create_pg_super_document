# numeric_add_opt_error

## Location
[src/backend/utils/adt/numeric.c:2883-2940](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L2883-L2940)

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
  - [make_result](../m/make_result.md) (numeric result construction)
  - [init_var_from_num](../i/init_var_from_num.md) (numeric variable initialization)
  - init_var (variable initialization)
  - [add_var](../a/add_var.md) (core addition arithmetic)
  - [make_result_opt_error](../m/make_result_opt_error.md) (result construction with error handling)
  - [free_var](../f/free_var.md) (memory cleanup)
- Called from (representative examples):
  - [numeric_add](numeric_add.md) (standard addition wrapper)
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md) (JSON path execution)
  - [timestamp_part_common](../t/timestamp_part_common.md) (timestamp arithmetic)
  - [timestamptz_part_common](../t/timestamptz_part_common.md) (timestamptz arithmetic)
  - [interval_part_common](../i/interval_part_common.md) (interval arithmetic)

## Notes and Other Information
- Core implementation function for all numeric addition operations in PostgreSQL
- Handles special IEEE-like arithmetic rules: NaN propagation, infinity arithmetic
- Inf + (-Inf) and (-Inf) + Inf both result in NaN as per mathematical convention
- Uses high-precision NumericVar arithmetic for exact decimal calculations
- Optional error handling allows integration with contexts that need custom error processing
- Memory management follows PostgreSQL conventions with proper variable cleanup
- Critical component of PostgreSQL's arbitrary precision numeric system
- Used both directly and indirectly throughout the database system for precise arithmetic