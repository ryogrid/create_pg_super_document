# initArrayResultAny

## Location
src/backend/utils/adt/arrayfuncs.c: 5770 - 5816

## Overview
Initializes an empty ArrayBuildStateAny structure that can accept either scalar or array inputs, automatically choosing the appropriate building strategy.

## Definition


## Detailed Description
This function provides a unified initialization interface for array building that can handle both scalar elements and array inputs. It examines the input_type to determine whether it represents a scalar type or an array type, then initializes the appropriate underlying state structure (ArrayBuildState for scalars or ArrayBuildStateArr for arrays).

The function uses get_array_type() to determine if the input_type is a scalar (has an associated array type) or is already an array type. Special handling is provided for int2vector and oidvector types, which are treated as scalars for consistency with get_promoted_array_type.

## Parameters / Member Variables
- : OID of the input datatype (can be either element type or array type)
- : Memory context where working state should be kept
- : Flag determining whether to create a separate memory context for array building

## Dependencies
- Functions called/Symbols referenced:
  - get_array_type
  - initArrayResultArr (for array inputs)
  - initArrayResult (for scalar inputs)
  - MemoryContextAlloc
  - OidIsValid
- Called from (representative examples):
  - ExecScanSubPlan
  - ExecSetParamPlan
  - accumArrayResultAny

## Notes and Other Information
- **Type detection**: Uses get_array_type() rather than get_element_type() for type detection
- **Special cases**: int2vector and oidvector are treated as scalars despite satisfying both type checks
- **State management**: The returned ArrayBuildStateAny contains either a scalarstate or arraystate pointer, but not both
- **Memory allocation**: The ArrayBuildStateAny structure itself is allocated in the same context as the underlying state
- **API unification**: Part of a three-function unified API: initArrayResultAny/accumArrayResultAny/makeArrayResultAny
- **Flexibility**: Allows the same code path to handle both scalar and array accumulation without prior knowledge of input type
- **Consistency**: Type detection logic matches get_promoted_array_type behavior for edge cases