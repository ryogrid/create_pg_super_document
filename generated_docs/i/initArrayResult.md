# initArrayResult

## Location
[src/backend/utils/adt/arrayfuncs.c:5281-5297](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L5281-L5297)

## Overview
Initializes an empty ArrayBuildState structure for accumulating array elements, providing a convenient interface for building arrays with known element type and memory management preferences.

## Definition


## Detailed Description
This function creates and initializes an ArrayBuildState structure for building arrays incrementally. It serves as a wrapper around initArrayResultWithSize() with sensible default initial array sizes. The function supports two memory management strategies: using a separate memory context for each array build state, or allocating directly within the provided context.

The function chooses different initial sizes based on the subcontext parameter:
- With subcontext=true: starts with 64 elements (suitable when each state has its own memory context)
- With subcontext=false: starts with 8 elements (conservative when sharing memory context)

This function implements the newer scheme for array building where you always get a non-NULL pointer that can be passed to makeArrayResult, resulting in an empty array if no elements were added.

## Parameters / Member Variables
- : OID of the array element type (must be a valid array element type)
- : Memory context where working state should be kept
- : Flag determining whether to use a separate memory context for this build state

## Dependencies
- Functions called/Symbols referenced:
  - [initArrayResultWithSize](initArrayResultWithSize.md) (performs the actual initialization with specified size)
- Called from (representative examples):
  - [array_agg_transfn](../a/array_agg_transfn.md) (aggregate function for building arrays)
  - [array_positions](../a/array_positions.md) (finds positions of elements in arrays)
  - [accumArrayResult](../a/accumArrayResult.md) (when initializing from NULL state)
  - [range_agg_transfn](../r/range_agg_transfn.md) (range aggregation function)
  - [xpath](../x/xpath.md) (XML path expression function)

## Notes and Other Information
- This function implements the preferred newer scheme for array building compared to the older NULL-pointer scheme
- Memory context strategy should be chosen based on use case:
  - Use subcontext=true when array build states have different lifetimes
  - Use subcontext=false when many concurrent small states exist (e.g., hash aggregation)
- Always returns a non-NULL ArrayBuildState pointer
- The returned state can be used with accumArrayResult() and makeArrayResult()
- Initial array size is automatically chosen based on memory management strategy