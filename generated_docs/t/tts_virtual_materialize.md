# tts_virtual_materialize

## Location
[src/backend/executor/execTuples.c:176-268](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L176-L268)

## Overview
Materializes a VirtualTupleTableSlot by copying all non-pass-by-value datums into the slot's memory context, ensuring the slot owns its data and doesn't depend on external references.

## Definition
```c
static void tts_virtual_materialize(TupleTableSlot *slot)
```

## Detailed Description
This function converts a virtual tuple slot from referencing external data to owning its own copies of all variable-length data. Virtual slots initially store only pointers to data that may exist elsewhere in memory. Materialization ensures the slot has its own persistent copies of this data in its memory context.

The materialization process occurs in two phases:
1. **Size Calculation**: Iterates through all attributes to compute the total memory required for all non-pass-by-value data
2. **Data Copying**: Allocates a single contiguous memory block and copies all variable-length data into it

The function handles expanded objects specially by flattening them during materialization, ensuring the materialized slot doesn't depend on the expanded object infrastructure.

## Parameters / Member Variables
- `slot`: A TupleTableSlot pointer that will be materialized (cast internally to VirtualTupleTableSlot)

## Dependencies
- Functions called/Symbols referenced:
  - [VirtualTupleTableSlot](../V/VirtualTupleTableSlot.md) (cast type)
  - TTS_SHOULDFREE (macro to check if slot should be freed)
  - VARATT_IS_EXTERNAL_EXPANDED (macro to check for expanded objects)
  - att_align_nominal (function to align data based on type requirements)
  - [DatumGetEOHP](../D/DatumGetEOHP.md) (macro to get expanded object header)
  - [EOH_get_flat_size](../E/EOH_get_flat_size.md) (function to get flattened size of expanded object)
  - att_addlength_datum (function to calculate attribute storage length)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (function to allocate memory)
  - TTS_FLAG_SHOULDFREE (flag indicating slot owns its data)
  - [ExpandedObjectHeader](../E/ExpandedObjectHeader.md) (type for expanded object headers)
  - [EOH_flatten_into](../E/EOH_flatten_into.md) (function to flatten expanded object)
- Called from (representative examples):
  - [tts_virtual_copyslot](tts_virtual_copyslot.md) (at src/backend/executor/execTuples.c:287)
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md) (at src/backend/executor/execTuples.c:1115)

## Notes and Other Information
- Returns early if the slot is already materialized (checked via TTS_SHOULDFREE flag)
- Optimizes by using a single memory allocation for all variable-length data, improving cache locality
- Handles both regular variable-length data and expanded objects (like expanded arrays or records)
- Pass-by-value attributes and NULL values are skipped since they don't require separate storage
- Sets the TTS_FLAG_SHOULDFREE flag to indicate the slot now owns its data
- The single contiguous allocation approach reduces memory fragmentation and allocation overhead