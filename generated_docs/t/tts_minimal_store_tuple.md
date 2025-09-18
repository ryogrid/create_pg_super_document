# tts_minimal_store_tuple

## Location
src/backend/executor/execTuples.c: 680 - 708

## Overview
Stores a MinimalTuple into a MinimalTupleTableSlot, setting up the slot's internal structures to reference the tuple data.

## Definition
static void tts_minimal_store_tuple(TupleTableSlot *slot, MinimalTuple mtup, bool shouldFree)

## Detailed Description
This function stores a MinimalTuple in a MinimalTupleTableSlot by first clearing any existing content, then setting up the slot's internal data structures to properly reference the new tuple. It creates a HeapTupleData header (minhdr) that points to the minimal tuple data with appropriate offset calculations. The function handles memory management flags to indicate whether the slot should free the tuple when cleared. The slot is marked as non-empty and ready for tuple access operations.

## Parameters / Member Variables
- `slot`: A TupleTableSlot pointer that must be a MinimalTupleTableSlot instance
- `mtup`: The MinimalTuple to store in the slot
- `shouldFree`: Boolean indicating whether the slot should free the tuple when cleared

## Dependencies
- Functions called/Symbols referenced:
  - MinimalTuple (parameter type)
  - MinimalTupleTableSlot (struct type cast)
  - tts_minimal_clear
  - TTS_SHOULDFREE, TTS_EMPTY (slot state macros)
  - TTS_FLAG_EMPTY, TTS_FLAG_SHOULDFREE (slot flags)
  - MINIMAL_TUPLE_OFFSET (offset constant)
  - HeapTupleHeader (type cast)
- Called from (representative examples):
  - ExecStoreMinimalTuple
  - ExecForceStoreMinimalTuple

## Notes and Other Information
- This is a static function internal to execTuples.c
- The function sets up a HeapTupleData header that creates a virtual view of the minimal tuple as a heap tuple
- Uses MINIMAL_TUPLE_OFFSET to properly align the tuple data with the expected heap tuple structure
- The t_self and t_tableOid fields are not set since they're not accessible through this slot type
- Memory management is controlled by the shouldFree parameter