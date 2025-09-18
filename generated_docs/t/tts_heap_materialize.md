# tts_heap_materialize

## Location
src/backend/executor/execTuples.c: 398 - 437

## Overview
Ensures that a heap tuple table slot contains a materialized (physically allocated) heap tuple by either forming a new tuple from deconstructed values or copying an existing tuple into the slot's memory context.

## Definition
```c
static void tts_heap_materialize(TupleTableSlot *slot)
```

## Detailed Description
This function guarantees that a HeapTupleTableSlot contains a physical HeapTuple allocated in the slot's memory context. It handles two scenarios: if the slot contains deconstructed values without a physical tuple, it constructs a new tuple using heap_form_tuple; if the slot already has a tuple but in a different memory context, it creates a copy using heap_copytuple.

The materialization process resets the deformation state (tts_nvalid and off) to ensure that subsequent tuple access operations will work with the materialized tuple rather than potentially stale deconstructed values. This is crucial for maintaining data consistency when the original tuple source might become invalid.

## Parameters / Member Variables
- `slot`: A TupleTableSlot pointer that will be materialized to contain a physical HeapTuple in its own memory context

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleTableSlot (cast target type)
  - TTS_EMPTY (macro for checking empty slots)
  - TTS_SHOULDFREE (macro for checking materialization status)
  - heap_form_tuple (creates new tuple from values array)
  - heap_copytuple (copies existing tuple)
  - TTS_FLAG_SHOULDFREE (flag indicating materialized tuple)
- Called from (representative examples):
  - tts_heap_get_heap_tuple
  - tts_heap_copy_heap_tuple
  - tts_heap_copy_minimal_tuple
  - slot_deform_heap_tuple

## Notes and Other Information
- The function is declared static, making it internal to the execTuples.c compilation unit
- Performs early return if the slot is already materialized (TTS_SHOULDFREE flag set)
- Switches to the slot's memory context during tuple creation/copying to ensure proper memory management
- Resets deformation state to prevent accessing stale deconstructed values after materialization
- Sets TTS_FLAG_SHOULDFREE to indicate the slot owns the tuple and should free it when cleared