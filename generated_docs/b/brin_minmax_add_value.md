# brin_minmax_add_value

## Location
[src/backend/access/brin/brin_minmax.c:64-136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax.c#L64-L136)

## Overview
Updates BRIN minmax index summary values by comparing a new heap tuple value against existing minimum and maximum boundaries, expanding the range if necessary.

## Definition
```c
Datum brin_minmax_add_value(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a core component of BRIN minmax operator class that maintains summary information for page ranges. It takes a new value from a heap tuple and compares it against the existing minimum and maximum values stored in the index tuple. If the new value falls outside the current range, the function updates the appropriate boundary (minimum or maximum) and returns true to indicate the summary was modified.

The function handles three cases:
1. If no values exist yet (bv_allnulls is true), it initializes both min and max to the new value
2. If the new value is less than the current minimum, it updates the minimum
3. If the new value is greater than the current maximum, it updates the maximum

The function uses comparison operators specific to the data type, retrieved through the strategy procedure cache, and properly handles memory management for both pass-by-value and pass-by-reference data types.

## Parameters / Member Variables
- `bdesc` (BrinDesc *): BRIN descriptor containing index metadata and type information
- `column` (BrinValues *): Current summary values for the column being updated
- `newval` (Datum): The new value to be incorporated into the summary
- `isnull` (bool): Indicates whether the new value is null (should always be false in this context)

## Dependencies
- Functions called/Symbols referenced:
  - [BrinDesc](../B/BrinDesc.md) (structure type)
  - [BrinValues](../B/BrinValues.md) (structure type) 
  - [datumCopy](../d/datumCopy.md) (function for copying datum values)
  - [minmax_get_strategy_procinfo](../m/minmax_get_strategy_procinfo.md) (function to get comparison procedures)
  - BTLessStrategyNumber (B-tree strategy constant)
  - BTGreaterStrategyNumber (B-tree strategy constant)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md) (function call with collation)
  - PG_USED_FOR_ASSERTS_ONLY (debugging macro)
  - PG_GET_COLLATION (macro to get collation)

- Called from (representative examples):
  - No direct callers found (likely called via function manager during index operations)

## Notes and Other Information
- The function assumes the new value is not null (asserted at the beginning)
- Memory management is handled carefully, freeing old values before copying new ones for pass-by-reference types
- The function returns a boolean indicating whether any updates were made to the summary
- Comparison operations are performed using the appropriate strategy procedures for the data type
- The function maintains the invariant that bv_values[0] contains the minimum and bv_values[1] contains the maximum