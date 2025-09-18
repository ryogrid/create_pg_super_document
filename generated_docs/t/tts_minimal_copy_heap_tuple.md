# tts_minimal_copy_heap_tuple

## Location
[src/backend/executor/execTuples.c:658-668](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L658-L668)

## Overview
Creates a HeapTuple copy from a MinimalTupleTableSlot by converting the slot's minimal tuple to a full heap tuple.

## Definition
static HeapTuple tts_minimal_copy_heap_tuple(TupleTableSlot *slot)

## Detailed Description
This function extracts a MinimalTuple from a MinimalTupleTableSlot and converts it to a HeapTuple structure. It first ensures the slot contains a materialized minimal tuple by calling tts_minimal_materialize if needed, then uses heap_tuple_from_minimal_tuple to create a new HeapTuple with proper system columns and metadata. This function bridges the gap between minimal tuple storage (optimized for space) and heap tuple representation (standard PostgreSQL tuple format).

## Parameters / Member Variables
- `slot`: A TupleTableSlot pointer that must be a MinimalTupleTableSlot instance

## Dependencies
- Functions called/Symbols referenced:
  - MinimalTupleTableSlot (struct type cast)
  - [tts_minimal_materialize](tts_minimal_materialize.md)
  - [heap_tuple_from_minimal_tuple](../h/heap_tuple_from_minimal_tuple.md)
  - MinimalTuple (type reference)
- Called from (representative examples):
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md)

## Notes and Other Information
- This is a static function internal to execTuples.c
- Returns a newly allocated HeapTuple that the caller is responsible for freeing
- The conversion adds system columns with default values since minimal tuples don't store them
- Used when code requires a full HeapTuple but the slot contains only a minimal tuple