# tts_minimal_materialize

## Location
[src/backend/executor/execTuples.c:586-633](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L586-L633)

## Overview
Ensures a minimal tuple slot has a materialized tuple in its own memory context, either by creating a new minimal tuple from slot values or by copying an existing minimal tuple to the slot's memory context.

## Definition
```c
static void tts_minimal_materialize(TupleTableSlot *slot)
```

## Detailed Description
This function is a core slot operation that ensures the minimal tuple contained in a slot is properly materialized and owned by the slot's memory context. Materialization is necessary when the slot's tuple data might become invalid (for example, when it points to memory that will be freed) or when the tuple needs to persist beyond the lifetime of its original context.

The function handles two scenarios:
1. If no minimal tuple exists yet, it creates one using `heap_form_minimal_tuple` from the slot's values and null flags
2. If a minimal tuple already exists but isn't owned by the slot (indicated by lack of TTS_SHOULDFREE flag), it creates a copy using `heap_copy_minimal_tuple`

After materialization, the function sets up the slot's tuple header structure to properly reference the minimal tuple data, adjusting for the MINIMAL_TUPLE_OFFSET that accounts for the difference between MinimalTuple and HeapTuple layouts.

## Parameters / Member Variables
- `slot`: Pointer to the TupleTableSlot to materialize

## Dependencies
- Functions called/Symbols referenced:
  - [MinimalTupleTableSlot](../M/MinimalTupleTableSlot.md) (type cast)
  - TTS_EMPTY (macro for checking if slot is empty)
  - TTS_SHOULDFREE (macro for checking if tuple should be freed)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (memory context management)
  - [heap_form_minimal_tuple](../h/heap_form_minimal_tuple.md) (creates minimal tuple from values)
  - [heap_copy_minimal_tuple](../h/heap_copy_minimal_tuple.md) (creates copy of existing minimal tuple)
  - TTS_FLAG_SHOULDFREE (flag indicating tuple ownership)
  - MINIMAL_TUPLE_OFFSET (offset constant for layout adjustment)
  - HeapTupleHeader (type cast)
- Called from (representative examples):
  - [tts_minimal_get_minimal_tuple](tts_minimal_get_minimal_tuple.md)
  - [tts_minimal_copy_heap_tuple](tts_minimal_copy_heap_tuple.md)
  - [tts_minimal_copy_minimal_tuple](tts_minimal_copy_minimal_tuple.md)
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md)

## Notes and Other Information
- This is a static function, only accessible within execTuples.c
- The function switches to the slot's memory context to ensure the materialized tuple is allocated in the correct context
- After materialization, the slot takes ownership of the tuple (TTS_FLAG_SHOULDFREE is set)
- The function resets tts_nvalid and off fields to force re-deformation of the tuple values
- The minhdr structure provides a HeapTuple-compatible view of the MinimalTuple for use with existing heap tuple functions
- MINIMAL_TUPLE_OFFSET accounts for the different header layouts between minimal tuples and heap tuples
- Materialization is a common operation when tuples need to be stored or passed between different execution contexts