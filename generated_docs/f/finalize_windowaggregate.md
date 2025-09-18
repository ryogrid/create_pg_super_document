# finalize_windowaggregate

## Location
src/backend/executor/nodeWindowAgg.c: 582 - 662

## Overview
Finalizes a window aggregate computation by applying the aggregate's final function (if any) to produce the final result value.

## Definition


## Detailed Description
This function is parallel to  in nodeAgg.c and handles the final step of window aggregate computation. It determines whether the aggregate has a final function and either calls that function with the current transition value or returns the transition value directly as the result. When a final function is present, it handles strict function semantics by checking for NULL arguments and avoiding calls to strict functions with NULL inputs. The function carefully manages memory contexts and uses  to ensure result values are properly formatted and read-only when needed.

## Parameters / Member Variables
- : The overall window aggregate execution state containing memory contexts and current aggregate context
- : Per-function state containing collation information needed for the final function call
- : Per-aggregate state containing the final function info, transition value, and type information
- : Output parameter to store the final computed result value
- : Output parameter to indicate whether the final result is NULL

## Dependencies
- Functions called/Symbols referenced:
  - InitFunctionCallInfoData
  - MakeExpandedObjectReadOnly
  - FunctionCallInvoke
- Called from (representative examples):
  - eval_windowaggregates

## Notes and Other Information
- If no final function exists ( is invalid), returns the transition value directly as the result
- When a final function exists, sets up function call info with the transition value as the first argument
- Fills any additional argument positions beyond the transition value with NULL values
- Handles strict final functions by returning NULL if any argument is NULL
- Sets  during final function calls to support AggCheckCallContext
- Uses  on both input transition values and output results to ensure proper memory management
- All computation is performed in the per-tuple memory context to ensure proper cleanup