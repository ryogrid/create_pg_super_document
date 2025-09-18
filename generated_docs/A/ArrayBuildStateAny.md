# ArrayBuildStateAny

## Location
[src/include/utils/array.h:226-231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/array.h#L226-L231)

## Overview
ArrayBuildStateAny is a polymorphic working state structure that can handle both scalar and array inputs for array construction, providing a unified interface for different types of array building operations.

## Definition
```c
typedef struct ArrayBuildStateAny
{
    /* Exactly one of these is not NULL: */
    ArrayBuildState *scalarstate;
    ArrayBuildStateArr *arraystate;
} ArrayBuildStateAny;
```

## Detailed Description
ArrayBuildStateAny serves as a discriminated union that allows array building functions to work with either scalar elements (via ArrayBuildState) or array inputs (via ArrayBuildStateArr) through a single interface. The structure contains exactly one non-NULL pointer, determining whether the operation is accumulating scalar values into an array or concatenating arrays together. This design provides flexibility for functions that need to handle different input types while maintaining type safety and avoiding code duplication. Functions like accumArrayResultAny() determine the appropriate operation mode and delegate to the corresponding specialized functions.

## Parameters / Member Variables
- `scalarstate`: Pointer to ArrayBuildState for scalar element accumulation (NULL if handling arrays)
- `arraystate`: Pointer to ArrayBuildStateArr for array concatenation operations (NULL if handling scalars)

## Dependencies
- Functions called/Symbols referenced:
  - [ArrayBuildState](ArrayBuildState.md) structure for scalar element handling
  - [ArrayBuildStateArr](ArrayBuildStateArr.md) structure for array concatenation
- Called from (representative examples):
  - [accumArrayResultAny](../a/accumArrayResultAny.md)() - adds elements/arrays using appropriate method
  - initArrayResultAny() - initializes based on input type
  - [makeArrayResultAny](../m/makeArrayResultAny.md)() - creates final array from accumulated state
  - [ExecScanSubPlan](../E/ExecScanSubPlan.md)() - executor functions that handle subplan results
  - [ExecSetParamPlan](../E/ExecSetParamPlan.md)() - executor functions for parameter handling

## Notes and Other Information
- Exactly one of the two member pointers must be non-NULL at any time, never both or neither
- The choice between scalar and array mode is determined at initialization based on the input data type
- Provides a unified interface for array building operations regardless of input type
- Used in executor functions where the input type may not be known at compile time
- Allows the same code path to handle both scalar aggregation and array concatenation scenarios
- The discrimination between modes happens automatically based on whether the input type is an array type or element type