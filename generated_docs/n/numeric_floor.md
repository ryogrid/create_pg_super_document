# numeric_floor

## Location
[src/backend/utils/adt/numeric.c:1673-1700](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L1673-L1700)

## Overview
Implements the floor function for PostgreSQL numeric data type, returning the largest integer less than or equal to the input numeric value.

## Definition

```c
Datum
numeric_floor(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that computes the floor (largest integer <= input) of a numeric value. It handles special numeric values (NaN and infinities) by returning them unchanged. For regular numeric values, it converts the input to an internal NumericVar representation, applies the floor operation via , and converts the result back to a Numeric datum for return.

The function follows PostgreSQL's standard function call convention using  and returns a  type that wraps the resulting Numeric value.

## Parameters / Member Variables
- Input parameter (accessed via ): The numeric value to apply floor operation to

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC - Extract numeric argument from function call
  - NUMERIC_IS_SPECIAL - Check for special values (NaN, infinity)
  - [duplicate_numeric](../d/duplicate_numeric.md) - Create copy of special numeric values
  - [init_var_from_num](../i/init_var_from_num.md) - Convert Numeric to NumericVar representation
  - [floor_var](../f/floor_var.md) - Perform floor operation on NumericVar
  - [make_result](../m/make_result.md) - Convert NumericVar back to Numeric
  - [free_var](../f/free_var.md) - Clean up NumericVar memory
  - PG_RETURN_NUMERIC - Return Numeric datum result
- Called from (representative examples):
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md) (in jsonpath execution)

## Notes and Other Information
- Located in src/backend/utils/adt/numeric.c:1673-1700
- Handles special numeric values (NaN, infinities) as a special case
- Uses PostgreSQL's internal NumericVar representation for computation
- Part of PostgreSQL's mathematical function suite for the numeric data type
- Follows PostgreSQL's memory management patterns with proper cleanup
- Complementary function to , providing the floor operation

## Simplified Source

```c
Datum
numeric_floor(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    Numeric res;
    NumericVar result;

    // Handle special values (NaN, infinity) - return as-is
    if (NUMERIC_IS_SPECIAL(num))
        PG_RETURN_NUMERIC(duplicate_numeric(num));

    // Convert to internal representation and apply floor
    init_var_from_num(num, &result);
    floor_var(&result, &result);

    // Convert back to Numeric and clean up
    res = make_result(&result);
    free_var(&result);

    PG_RETURN_NUMERIC(res);
}
```