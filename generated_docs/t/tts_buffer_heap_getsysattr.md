# tts_buffer_heap_getsysattr

## Location
src/backend/executor/execTuples.c: 759 - 778

## Overview
Retrieves the value of a system attribute from a buffer-backed heap tuple table slot, with validation for materialized tuples.

## Definition
static Datum tts_buffer_heap_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)

## Detailed Description
tts_buffer_heap_getsysattr extracts system attribute values from buffer-backed heap tuple table slots. System attributes are special PostgreSQL-maintained columns like ctid, xmin, xmax, and cmin that provide metadata about tuples. The function requires that the tuple be materialized (copied from the buffer into memory) before system attributes can be accessed. If the slot contains a non-materialized tuple, the function raises an error indicating that system columns cannot be retrieved in that context. When the tuple is properly materialized, it delegates to heap_getsysattr to perform the actual attribute extraction.

## Parameters / Member Variables
- : A pointer to the TupleTableSlot containing the buffer-backed heap tuple
- : The attribute number of the system column to retrieve (negative values for system attributes)
- : A pointer to a boolean that will be set to indicate if the attribute value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - BufferHeapTupleTableSlot (cast target type)
  - TTS_EMPTY (slot state check macro)
  - [heap_getsysattr](../h/heap_getsysattr.md) (core system attribute extraction function)
- Called from (representative examples):
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md) (as part of vtable operations)

## Notes and Other Information
- This is a static function, accessible only within execTuples.c
- Part of the tuple table slot operations vtable pattern for buffer-backed heap slots
- Includes safety assertions to prevent access to empty slots
- Requires tuple materialization before system attribute access - raises ERRCODE_FEATURE_NOT_SUPPORTED error otherwise
- System attributes include ctid (tuple identifier), xmin/xmax (transaction IDs), cmin/cmax (command IDs)
- Returns a Datum value that contains the system attribute data
- The isnull parameter is set by the underlying heap_getsysattr function