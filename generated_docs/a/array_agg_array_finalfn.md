# array_agg_array_finalfn

## Location
[src/backend/utils/adt/array_userfuncs.c:1192-1224](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_userfuncs.c#L1192-L1224)

## Overview
Finalizes the array_agg aggregation by converting the accumulated ArrayBuildStateArr into the final PostgreSQL array result.

## Definition
Datum array_agg_array_finalfn(PG_FUNCTION_ARGS)

## Detailed Description
This function serves as the final function for the array_agg aggregate operation. It takes the accumulated ArrayBuildStateArr state (which contains all the individual arrays that have been aggregated) and converts it into the final PostgreSQL array result using makeArrayResultArr. The function handles the case where no input values were provided (returning NULL) and ensures proper memory context handling. Unlike some other final functions, this one deliberately does not release the ArrayBuildStateArr because aggregate final functions may be re-executed, leaving memory cleanup to nodeAgg.c when it's safe to reset the aggregation context.

## Parameters / Member Variables
- : Function call information structure containing the ArrayBuildStateArr pointer as argument
- Returns: Final aggregated array as a Datum

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - makeArrayResultArr
  - PG_RETURN_DATUM
- Called from (representative examples):
  - No direct references found (used as aggregate final function)

## Notes and Other Information
- Cannot be called directly due to internal-type argument restriction
- Used as the final function for array_agg aggregate operations
- Does not release the ArrayBuildStateArr to allow for potential re-execution
- Returns NULL if no input values were aggregated
- Memory cleanup is handled by nodeAgg.c when the aggregation context is reset
- Creates final result in CurrentMemoryContext rather than aggregate context