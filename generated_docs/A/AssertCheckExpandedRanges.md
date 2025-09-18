# AssertCheckExpandedRanges

## Location
[src/backend/access/brin/brin_minmax_multi.c:426-485](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L426-L485)

## Overview
AssertCheckExpandedRanges is a debugging function that validates the correctness and ordering of ExpandedRange arrays used during range reduction operations in BRIN minmax-multi indexes.

## Definition


## Detailed Description
This function performs comprehensive validation of an array of ExpandedRange structures, which are used when reducing the number of ranges by combining adjacent or overlapping ranges in BRIN minmax-multi indexes. The function validates two critical properties:

1. **Individual range validity**: Each range must be internally consistent, where collapsed ranges have equal min/max values, and non-collapsed ranges have min < max
2. **Inter-range ordering**: Consecutive ranges must be properly ordered and non-overlapping, with the maximum value of each range being less than the minimum value of the next range

The function dynamically retrieves the appropriate comparison functions (equality and less-than) based on the attribute type and uses them to perform the validation. This ensures type-specific comparison semantics are correctly applied.

## Parameters / Member Variables
- : Pointer to BrinDesc structure containing BRIN index metadata
- : OID of the collation to use for comparison operations
- : Attribute number within the BRIN index
- : Form_pg_attribute structure describing the attribute
- : Array of ExpandedRange structures to validate
- : Number of ranges in the array

## Dependencies
- Functions called/Symbols referenced:
  - [minmax_multi_get_strategy_procinfo](../m/minmax_multi_get_strategy_procinfo.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
- Data structures referenced:
  - [BrinDesc](../B/BrinDesc.md)
  - [ExpandedRange](../E/ExpandedRange.md)
  - BTEqualStrategyNumber
  - BTLessStrategyNumber
- Called from (representative examples):
  - [ensure_free_space_in_buffer](../e/ensure_free_space_in_buffer.md)
  - [brin_minmax_multi_union](../b/brin_minmax_multi_union.md)

## Notes and Other Information
- This function only executes when USE_ASSERT_CHECKING is defined (debug builds)
- Uses PostgreSQL's strategy number system to obtain type-specific comparison functions
- Handles both collapsed ranges (single points) and normal ranges (intervals)
- Critical for ensuring correctness during range consolidation operations
- Part of the validation framework for BRIN minmax-multi index operations
- Located in src/backend/access/brin/brin_minmax_multi.c:426-485