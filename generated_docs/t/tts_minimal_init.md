# tts_minimal_init

## Location
src/backend/executor/execTuples.c: 508 - 519

## Overview
Initializes a MinimalTupleTableSlot by setting up the heap tuple pointer to enable attribute access on the minimal tuple as if it were a heap tuple.

## Definition
```c
static void tts_minimal_init(TupleTableSlot *slot)
```

## Detailed Description
This function is part of the TupleTableSlotOps implementation for MinimalTupleTableSlot. It performs the essential initialization step of setting up the internal heap tuple pointer (mslot->tuple) to point to the minimal header (mslot->minhdr). This clever design allows the minimal tuple to be accessed using the same attribute access methods as regular heap tuples, providing a unified interface while maintaining the memory efficiency of minimal tuples.

## Parameters / Member Variables
- `slot`: A TupleTableSlot pointer that should be a MinimalTupleTableSlot to be initialized

## Dependencies
- Functions called/Symbols referenced:
  - MinimalTupleTableSlot (type cast)
- Called from (representative examples):
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md)

## Notes and Other Information
- This is a static function specific to minimal tuple table slot operations
- Part of the TupleTableSlotOps implementation for MinimalTupleTableSlot
- The initialization creates a bridge between minimal tuple storage and heap tuple access patterns
- The minhdr member serves as a HeapTuple header that points to the minimal tuple data
- Located in src/backend/executor/execTuples.c:508-519
- Essential for the abstraction that allows minimal tuples to be treated like heap tuples for attribute access