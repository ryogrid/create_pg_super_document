# ExecDropSingleTupleTableSlot

## Location
[src/backend/executor/execTuples.c:1341-1375](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L1341-L1375)

## Overview
Releases and deallocates a TupleTableSlot created with MakeSingleTupleTableSlot, properly cleaning up all associated resources.

## Definition

```c
void
ExecDropSingleTupleTableSlot(TupleTableSlot *slot)
```
## Detailed Description
ExecDropSingleTupleTableSlot is the cleanup counterpart to MakeSingleTupleTableSlot. It properly releases a standalone TupleTableSlot by performing the same processing as ExecResetTupleTable does for individual slots. The function systematically cleans up all resources associated with the slot including the tuple data, tuple descriptor, value arrays, and the slot structure itself.

This function should ONLY be used on slots created with MakeSingleTupleTableSlot and never on slots that are part of a tuple table list, as those are managed by the executor's tuple table system.

## Parameters
- : TupleTableSlot to be released and deallocated

## Dependencies
- Functions called/Symbols referenced:
  - [ExecClearTuple](ExecClearTuple.md)
  - ReleaseTupleDesc
  - TTS_FIXED
  - [pfree](../p/pfree.md)

- Called from (representative examples):
  - [systable_endscan](../s/systable_endscan.md)
  - [CatalogIndexInsert](../C/CatalogIndexInsert.md)
  - [ATRewriteTable](../A/ATRewriteTable.md)
  - [afterTriggerInvokeEvents](../a/afterTriggerInvokeEvents.md)
  - [ExecEndModifyTable](ExecEndModifyTable.md)
  - [compute_index_stats](../c/compute_index_stats.md)

## Notes and Other Information
- WARNING: Should never be used on slots that are part of a tuple table list - only on standalone slots
- Follows the same cleanup pattern as ExecResetTupleTable for individual slots
- Includes memory management for both fixed and variable-sized slot types via TTS_FIXED check
- Releases the tuple descriptor reference and frees value/null arrays for non-fixed slots
- Extensively used throughout PostgreSQL for cleanup in catalog operations, indexing, replication, and statistical analysis
- Essential for preventing memory leaks in operations using standalone tuple slots

## Simplified Source

```c
// Simplified version of ExecDropSingleTupleTableSlot
void ExecDropSingleTupleTableSlot(TupleTableSlot *slot) {
    // Validate slot type
    Assert(IsA(slot, TupleTableSlot));

    // Clear any tuple data in the slot
    ExecClearTuple(slot);

    // Release slot-specific resources
    slot->tts_ops->release(slot);

    // Release tuple descriptor reference
    if (slot->tts_tupleDescriptor)
        ReleaseTupleDesc(slot->tts_tupleDescriptor);

    // Free value and null arrays for non-fixed slots
    if (!TTS_FIXED(slot)) {
        if (slot->tts_values)
            pfree(slot->tts_values);
        if (slot->tts_isnull)
            pfree(slot->tts_isnull);
    }

    // Free the slot structure itself
    pfree(slot);
}
```

Key simplifications made:
- Function is already well-structured for cleanup operations
- Maintains proper resource release order: data → slot resources → descriptor → arrays → slot
- Preserves type checking and memory management for different slot types
- Essential counterpart to MakeSingleTupleTableSlot for preventing memory leaks