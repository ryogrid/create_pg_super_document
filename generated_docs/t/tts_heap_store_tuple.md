# tts_heap_store_tuple

## Location
[src/backend/executor/execTuples.c:486-507](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L486-L507)

## Overview
Stores a HeapTuple into a heap tuple table slot, initializing the slot's state and managing memory ownership flags.

## Definition
```c
static void tts_heap_store_tuple(TupleTableSlot *slot, HeapTuple tuple, bool shouldFree)
```

## Detailed Description
This function is responsible for storing a HeapTuple into a HeapTupleTableSlot. It first clears any existing content in the slot, then initializes the slot with the new tuple. The function sets up the slot's metadata including the tuple identifier (TID) and manages memory ownership through the shouldFree flag. This is a core operation in PostgreSQL's tuple table slot system for heap-based storage.

## Parameters / Member Variables
- `slot`: A TupleTableSlot pointer that should be a HeapTupleTableSlot to store the tuple in
- `tuple`: The HeapTuple to be stored in the slot
- `shouldFree`: Boolean flag indicating whether the slot should take ownership and free the tuple when cleared

## Dependencies
- Functions called/Symbols referenced:
  - [HeapTupleTableSlot](../H/HeapTupleTableSlot.md) (type cast)
  - [tts_heap_clear](tts_heap_clear.md)
  - TTS_FLAG_EMPTY
  - TTS_FLAG_SHOULDFREE
- Called from (representative examples):
  - [ExecStoreHeapTuple](../E/ExecStoreHeapTuple.md)

## Notes and Other Information
- This is a static function specific to heap tuple table slot operations
- The function manages tuple ownership through the TTS_FLAG_SHOULDFREE flag
- Resets tts_nvalid to 0, indicating that no columns have been extracted yet
- Sets the slot's TID to the tuple's self-identifier for tuple identification
- Part of the tuple table slot abstraction layer in PostgreSQL's executor
- Located in src/backend/executor/execTuples.c:486-507