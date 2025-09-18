# makeInt128AggStateCurrentContext

## Location
[src/backend/utils/adt/numeric.c:5520-5533](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L5520-L5533)

## Overview
Creates and initializes a state structure for 128-bit aggregate functions in the current memory context, similar to makeInt128AggState but without aggregate context management.

## Definition
```c
static Int128AggState *makeInt128AggStateCurrentContext(bool calcSumX2)
```

## Detailed Description
This function provides a simplified version of `makeInt128AggState()` that allocates the `Int128AggState` structure in the current memory context rather than switching to the aggregate context. This is useful when the caller wants to manage the memory context themselves or when the state needs to be allocated in a different context than the standard aggregate context.

## Parameters / Member Variables
- `calcSumX2`: Boolean flag indicating whether the aggregate should calculate sum of squares in addition to sum and count

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md): Allocates zero-initialized memory in current context
  - [Int128AggState](../I/Int128AggState.md): The state structure type being allocated
- Called from (representative examples):
  - Used via `makePolyNumAggStateCurrentContext` macro for polynomial numeric aggregates

## Notes and Other Information
- This is a static function, meaning it's only visible within the numeric.c file
- Unlike `makeInt128AggState()`, this function does not perform aggregate context validation or memory context switching
- The function is aliased as `makePolyNumAggStateCurrentContext` through a macro definition
- Simpler than its counterpart as it assumes the caller has already set up the appropriate memory context
- Primarily used in scenarios where more granular control over memory management is required