# tts_minimal_get_minimal_tuple

## Location
[src/backend/executor/execTuples.c:647-657](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L647-L657)

## Overview
Retrieves the MinimalTuple from a MinimalTupleTableSlot, materializing it if necessary.

## Definition

```c
static MinimalTuple
tts_minimal_get_minimal_tuple(TupleTableSlot *slot)
```
## Detailed Description
This function is a specialized accessor for MinimalTupleTableSlot that ensures the slot contains a materialized minimal tuple before returning it. The function casts the generic TupleTableSlot to a MinimalTupleTableSlot and checks if the mintuple field is populated. If the minimal tuple is not yet materialized (mintuple is NULL), it calls tts_minimal_materialize to create one. This lazy materialization approach optimizes memory usage by only creating the minimal tuple when actually needed.

## Parameters / Member Variables
- `slot`: A TupleTableSlot pointer that must be a MinimalTupleTableSlot instance

## Dependencies
- Functions called/Symbols referenced:
  - [MinimalTupleTableSlot](../M/MinimalTupleTableSlot.md) (struct type cast)
  - [tts_minimal_materialize](tts_minimal_materialize.md)
- Called from (representative examples):
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md)

## Notes and Other Information
- This is a static function internal to execTuples.c
- The function assumes the input slot is actually a MinimalTupleTableSlot
- Uses lazy materialization pattern for performance optimization
- Returns the minimal tuple directly without copying, so caller should not modify it