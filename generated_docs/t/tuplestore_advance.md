# tuplestore_advance

## Location
[src/backend/utils/sort/tuplestore.c:1110-1134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplestore.c#L1110-L1134)

## Overview
Exported function that advances the position in a tuplestore without actually fetching the tuple data, useful for skipping over tuples when only position advancement is needed.

## Definition
```c
bool tuplestore_advance(Tuplestorestate *state, bool forward)
```

## Detailed Description
This function provides a way to advance the current position in a tuplestore without actually retrieving the tuple data. It's essentially a lightweight version of `tuplestore_gettuple` that discards the tuple data immediately after fetching it. The function is designed for scenarios where you need to skip over tuples without processing them, such as advancing to a specific position or skipping unwanted rows.

The implementation leverages the existing `tuplestore_gettuple` infrastructure but immediately frees any allocated tuple data. While the comments suggest this could be optimized to avoid palloc/pfree overhead, the current implementation prioritizes code reuse and simplicity.

## Parameters / Member Variables
- `state`: Pointer to the Tuplestorestate structure representing the tuplestore to advance
- `forward`: Boolean indicating the direction of advancement (true for forward, false for backward)

## Dependencies
- Functions called/Symbols referenced:
  - tuplestore_gettuple
  - [pfree](../p/pfree.md) (when should_free is true)
- Called from (representative examples):
  - [CteScanNext](../C/CteScanNext.md)
  - [ExecMaterial](../E/ExecMaterial.md)
  - [window_gettupleslot](../w/window_gettupleslot.md)

## Notes and Other Information
- Returns true if a tuple was available and the position was successfully advanced, false if no more tuples are available
- The function properly handles memory cleanup by calling pfree when the tuple requires freeing
- More efficient than calling `tuplestore_gettuple` directly when the tuple data is not needed
- Could potentially be optimized further to avoid the palloc/pfree overhead mentioned in comments
- Used primarily in executor nodes that need to skip tuples during scanning operations