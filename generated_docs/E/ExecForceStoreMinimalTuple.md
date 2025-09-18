# ExecForceStoreMinimalTuple

## Location
[src/backend/executor/execTuples.c:1599-1638](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L1599-L1638)

## Overview
Stores a MinimalTuple into any kind of TupleTableSlot, performing automatic type conversion when the target slot is not a minimal tuple slot type.

## Definition
```c
void ExecForceStoreMinimalTuple(MinimalTuple mtup, TupleTableSlot *slot, bool shouldFree)
```

## Detailed Description
ExecForceStoreMinimalTuple provides a universal interface for storing minimal tuples into any type of TupleTableSlot. When the target slot is already a minimal tuple slot, it uses the optimized tts_minimal_store_tuple function directly. For other slot types, it performs a conversion process:

1. Creates a temporary HeapTupleData structure by adjusting the minimal tuple pointer and length
2. Uses heap_deform_tuple to extract individual column values from the converted heap tuple representation
3. Stores the deformed values as a virtual tuple in the target slot

This approach allows minimal tuples to be stored in any slot type while maintaining proper memory management and data integrity.

## Parameters / Member Variables
- `mtup`: The MinimalTuple to be stored in the slot
- `slot`: The target TupleTableSlot where the minimal tuple will be stored (can be any slot type)
- `shouldFree`: Boolean flag indicating whether the original minimal tuple should be freed after storage

## Dependencies
- Functions called/Symbols referenced:
  - MinimalTuple (type)
  - TTS_IS_MINIMALTUPLE (type checking macro)
  - [tts_minimal_store_tuple](../t/tts_minimal_store_tuple.md) (optimized minimal tuple storage)
  - [HeapTupleData](../H/HeapTupleData.md) (temporary heap tuple structure)
  - ExecClearTuple (slot clearing function)
  - MINIMAL_TUPLE_OFFSET (offset constant for conversion)
  - HeapTupleHeader (heap tuple header type)
  - [heap_deform_tuple](../h/heap_deform_tuple.md) (tuple deformation function)
  - [ExecStoreVirtualTuple](ExecStoreVirtualTuple.md) (virtual tuple storage)
  - ExecMaterializeSlot (slot materialization)
- Called from (representative examples):
  - [ExecParallelHashJoinOuterGetTuple](ExecParallelHashJoinOuterGetTuple.md)
  - [ExecParallelHashJoinNewBatch](ExecParallelHashJoinNewBatch.md)
  - [ExecHashJoinGetSavedTuple](ExecHashJoinGetSavedTuple.md)

## Notes and Other Information
- This function bridges the gap between minimal tuples and other slot types through format conversion
- The conversion process creates a temporary HeapTupleData structure without copying the actual tuple data, using pointer arithmetic to adjust for the MINIMAL_TUPLE_OFFSET
- For minimal tuple slots, it uses the optimized direct storage path via tts_minimal_store_tuple
- When shouldFree is true and conversion is needed, the slot is materialized before freeing the original tuple to ensure data persistence
- Primarily used in hash join operations where tuples may be stored and retrieved from different slot types
- More expensive than ExecStoreMinimalTuple when the slot type is guaranteed to be minimal, but provides universal compatibility
- The conversion leverages the fact that minimal tuples are essentially heap tuples with a reduced header, allowing efficient format translation