# tts_virtual_copyslot

## Location
[src/backend/executor/execTuples.c:269-290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L269-L290)

## Overview
Copies all attribute values and null indicators from a source TupleTableSlot to a destination VirtualTupleTableSlot, ensuring the destination owns independent copies of all data.

## Definition
```c
static void tts_virtual_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
```

## Detailed Description
This function performs a complete copy operation from any type of source slot to a virtual destination slot. The copying process involves several steps:

1. **Preparation**: Clears the destination slot and ensures all attributes in the source slot are deformed (extracted and available)
2. **Value Copying**: Copies all attribute values and null indicators from source to destination arrays
3. **Metadata Update**: Sets the destination slot's validity count and clears the empty flag
4. **Materialization**: Calls `tts_virtual_materialize()` to ensure the destination slot owns independent copies of all variable-length data

The materialization step is crucial because it ensures the destination slot doesn't depend on memory that might be deallocated when the source slot is cleared or destroyed.

## Parameters / Member Variables
- `dstslot`: The destination TupleTableSlot that will receive the copied data (must be a VirtualTupleTableSlot)
- `srcslot`: The source TupleTableSlot from which data will be copied (can be any slot type)

## Dependencies
- Functions called/Symbols referenced:
  - [tts_virtual_clear](tts_virtual_clear.md) (function to clear virtual slot)
  - [slot_getallattrs](../s/slot_getallattrs.md) (function to ensure all source attributes are deformed)
  - TTS_FLAG_EMPTY (flag indicating slot is empty)
  - [tts_virtual_materialize](tts_virtual_materialize.md) (function to materialize slot data)
- Called from (representative examples):
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md) (at src/backend/executor/execTuples.c:1117)

## Notes and Other Information
- This is a static function only accessible within execTuples.c
- The function assumes the destination slot is a VirtualTupleTableSlot
- The source slot can be of any type since `slot_getallattrs()` handles the deforming
- The final materialization step is essential for data independence - without it, the destination would only hold pointers to the source data
- After copying, the destination slot is fully independent and can outlive the source slot
- The copying includes both the actual values and the null indicators for each attribute

## Simplified Source

```c
static void tts_virtual_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
    TupleDesc srcdesc = srcslot->tts_tupleDescriptor;

    // Clear destination slot
    tts_virtual_clear(dstslot);

    // Ensure all source attributes are available
    slot_getallattrs(srcslot);

    // Copy all attribute values and null indicators
    for (int natt = 0; natt < srcdesc->natts; natt++) {
        dstslot->tts_values[natt] = srcslot->tts_values[natt];
        dstslot->tts_isnull[natt] = srcslot->tts_isnull[natt];
    }

    // Update destination slot metadata
    dstslot->tts_nvalid = srcdesc->natts;
    dstslot->tts_flags &= ~TTS_FLAG_EMPTY;

    // Ensure destination owns independent copies of all data
    tts_virtual_materialize(dstslot);
}
```