# ExecStoreHeapTuple

## Location
src/backend/executor/execTuples.c: 1439 - 1478

## Overview
Stores a physical HeapTuple into a specified TTSOpsHeapTuple type slot in the tuple table, with configurable memory ownership.

## Definition


## Detailed Description
ExecStoreHeapTuple is used to store an on-the-fly physical tuple into a specified slot in the tuple table. This function is optimized for TTSOpsHeapTuple type slots and provides efficient storage of HeapTuple structures. The function manages memory ownership through the shouldFree parameter, allowing flexible control over tuple lifecycle management.

The function performs type checking to ensure the target slot is a heap tuple slot and delegates the actual storage operation to tts_heap_store_tuple. It also preserves the table OID from the source tuple in the target slot.

## Parameters
- : HeapTuple to store in the slot
- : TupleTableSlot of TTSOpsHeapTuple type to store the tuple in
- : Boolean flag indicating whether ExecClearTuple should pfree() the tuple when done with it

## Dependencies
- Functions called/Symbols referenced:
  - TTS_IS_HEAPTUPLE
  - [tts_heap_store_tuple](../t/tts_heap_store_tuple.md)

- Called from (representative examples):
  - [CatalogIndexInsert](../C/CatalogIndexInsert.md)
  - [compute_index_stats](../c/compute_index_stats.md)
  - [ExecForceStoreHeapTuple](ExecForceStoreHeapTuple.md)
  - [setop_retrieve_direct](../s/setop_retrieve_direct.md)
  - [pgoutput_change](../p/pgoutput_change.md)
  - [tts_heap_copyslot](../t/tts_heap_copyslot.md)

## Notes and Other Information
- shouldFree is typically set to 'true' for tuples constructed on-the-fly
- shouldFree can be 'false' when the tuple is held in a lower-level executor slot that retains ownership
- IMPORTANT: When shouldFree is false, ensure the upper-level node loses interest before the lower-level one
- For uncertain ownership scenarios, use heap_copytuple and let the upper-level slot assume ownership
- For non-guaranteed TTSOpsHeapTuple slots, use the more expensive ExecForceStoreHeapTuple()
- Returns the passed-in slot pointer for convenience
- Used extensively in catalog operations, indexing, replication, and statistical analysis
- Part of PostgreSQL's tuple table slot system for efficient tuple management