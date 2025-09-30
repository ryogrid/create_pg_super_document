# tts_minimal_clear

## Location
[src/backend/executor/execTuples.c:525-542](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L525-L542)

## Overview
Clears a MinimalTupleTableSlot by freeing its minimal tuple if owned and resetting all slot state to empty.

## Definition
```c
static void tts_minimal_clear(TupleTableSlot *slot)
```

## Detailed Description
This function is part of the TupleTableSlotOps implementation for MinimalTupleTableSlot. It performs a complete cleanup of the slot, checking if the slot owns the minimal tuple memory (via TTS_SHOULDFREE flag) and freeing it if necessary using heap_free_minimal_tuple. After memory cleanup, it resets all slot state including validity count, flags, tuple identifier, and internal pointers, effectively returning the slot to an empty, reusable state.

## Parameters / Member Variables
- `slot`: A TupleTableSlot pointer that should be a MinimalTupleTableSlot to be cleared

## Dependencies
- Functions called/Symbols referenced:
  - [MinimalTupleTableSlot](../M/MinimalTupleTableSlot.md) (type cast)
  - TTS_SHOULDFREE
  - [heap_free_minimal_tuple](../h/heap_free_minimal_tuple.md)
  - TTS_FLAG_SHOULDFREE
  - TTS_FLAG_EMPTY
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md)
- Called from (representative examples):
  - [tts_minimal_store_tuple](tts_minimal_store_tuple.md)
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md)

## Notes and Other Information
- This is a static function specific to minimal tuple table slot operations
- Part of the TupleTableSlotOps implementation for MinimalTupleTableSlot
- Properly manages memory by checking ownership flags before freeing
- Resets tts_nvalid to 0, indicating no valid extracted columns
- Sets TTS_FLAG_EMPTY to mark the slot as containing no tuple
- Invalidates the tuple identifier using ItemPointerSetInvalid
- Resets internal offset tracking (mslot->off) and tuple pointer
- Located in src/backend/executor/execTuples.c:525-542
- Essential for proper resource management in the tuple table slot system

## Simplified Source

```c
static void tts_minimal_clear(TupleTableSlot *slot)
{
    MinimalTupleTableSlot *mslot = (MinimalTupleTableSlot *) slot;

    // Free minimal tuple if this slot owns it
    if (TTS_SHOULDFREE(slot)) {
        heap_free_minimal_tuple(mslot->mintuple);
        slot->tts_flags &= ~TTS_FLAG_SHOULDFREE;
    }

    // Reset slot to empty state
    slot->tts_nvalid = 0;
    slot->tts_flags |= TTS_FLAG_EMPTY;
    ItemPointerSetInvalid(&slot->tts_tid);
    mslot->off = 0;
    mslot->mintuple = NULL;
}
```