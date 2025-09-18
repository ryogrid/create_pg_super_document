# tts_virtual_copyslot

## Location
src/backend/executor/execTuples.c: 269 - 290

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
  - tts_virtual_clear (function to clear virtual slot)
  - slot_getallattrs (function to ensure all source attributes are deformed)
  - TTS_FLAG_EMPTY (flag indicating slot is empty)
  - tts_virtual_materialize (function to materialize slot data)
- Called from (representative examples):
  - slot_deform_heap_tuple (at src/backend/executor/execTuples.c:1117)

## Notes and Other Information
- This is a static function only accessible within execTuples.c
- The function assumes the destination slot is a VirtualTupleTableSlot
- The source slot can be of any type since `slot_getallattrs()` handles the deforming
- The final materialization step is essential for data independence - without it, the destination would only hold pointers to the source data
- After copying, the destination slot is fully independent and can outlive the source slot
- The copying includes both the actual values and the null indicators for each attribute