# ArrayMapState

## Location
[src/include/utils/array.h:251-255](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/array.h#L251-L255)

## Overview
ArrayMapState is a workspace structure that provides private state needed by the array_map() function to cache type metadata for efficient array element transformations.

## Definition

```c
typedef struct ArrayMapState
{
	ArrayMetaState inp_extra;
	ArrayMetaState ret_extra;
} ArrayMapState;
```
## Detailed Description
ArrayMapState serves as a workspace for the array_map() function, which transforms arrays through arbitrary expressions. This structure optimizes performance by caching type metadata for both input and output array elements across multiple calls. The structure must be zeroed by the caller before the first use and should not be modified after that. While it's legitimate to pass a freshly-zeroed ArrayMapState on each call, better performance is achieved when the state is preserved across a series of calls to array_map().

The structure contains two ArrayMetaState members that cache different type information: one for the input array's element type and another for the return array's element type. This separation allows array_map() to handle transformations where the input and output element types differ.

## Parameters / Member Variables
- `inp_extra`: ArrayMetaState structure that caches metadata about the input array's element type, including type length, alignment, and whether values are passed by value
- `ret_extra`: ArrayMetaState structure that caches metadata about the output array's element type, used for constructing the result array with proper type characteristics
## Dependencies
- Functions called/Symbols referenced:
  - [ArrayMetaState](ArrayMetaState.md)
- Called from (representative examples):
  - [array_map](../a/array_map.md)
  - [ExecInitExprRec](../E/ExecInitExprRec.md)
  - [ExprEvalStep](../E/ExprEvalStep.md) (used in expression evaluation context)

## Notes and Other Information
- The structure is designed specifically for performance optimization in array transformations
- Caller must ensure the structure is zeroed before first use
- Type metadata is looked up only once per series of calls, assuming element types don't change
- The structure enables efficient handling of arrays where input and output element types may differ
- Used primarily in PostgreSQL's expression evaluation system for array operations
- Located in src/include/utils/array.h:251-255