# accumArrayResultAny

## Location
src/backend/utils/adt/arrayfuncs.c: 5817 - 5844

## Overview
accumArrayResultAny accumulates one input value for building an array result, handling both scalar element inputs and array inputs in a unified interface.

## Definition


## Detailed Description
This function provides a unified interface for accumulating array elements during array construction, handling both scalar elements and array inputs. It maintains state in an ArrayBuildStateAny structure and delegates to appropriate specialized accumulation functions based on whether the input is scalar or array data. The function automatically initializes the state on the first call if astate is NULL.

## Parameters / Member Variables
- `astate`: Working state for array building (can be NULL on first call, will be initialized automatically)
- `dvalue`: The new input value to append to the array being built
- `disnull`: Boolean indicating whether the input value is NULL
- `input_type`: OID of the input datatype (either element type or array type)
- `rcontext`: Memory context where working state should be allocated

## Dependencies
- Functions called/Symbols referenced:
  - initArrayResultAny (for state initialization when astate is NULL)
  - accumArrayResult (for scalar element accumulation)
  - accumArrayResultArr (for array input accumulation)
  - ArrayBuildStateAny (state structure type)
- Called from (representative examples):
  - ExecScanSubPlan (in nodeSubplan.c:395)
  - ExecSetParamPlan (in nodeSubplan.c:1162)

## Notes and Other Information
- This function serves as a polymorphic wrapper that can handle both scalar and array inputs
- The function automatically determines whether to use scalar or array accumulation based on the state structure
- Memory management is handled through the provided memory context
- The function is designed to be called iteratively to build up array results element by element