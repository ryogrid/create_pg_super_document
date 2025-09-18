# tts_virtual_is_current_xact_tuple

## Location
[src/backend/executor/execTuples.c:157-175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L157-L175)

## Overview
A function that handles queries for transaction information on VirtualTupleTableSlots, which do not store transaction data, by raising an error to indicate this operation is not supported.

## Definition
```c
static bool tts_virtual_is_current_xact_tuple(TupleTableSlot *slot)
```

## Detailed Description
This function is part of the VirtualTupleTableSlot implementation in PostgreSQL. VirtualTupleTableSlots are a type of tuple slot that stores values directly in memory arrays rather than as physical tuple storage. Since they don't have associated storage tuples, they lack transaction information that would normally be available with heap tuples.

The function always throws an error with code `ERRCODE_FEATURE_NOT_SUPPORTED` and the message "don't have transaction information for this type of tuple" when called, as virtual slots fundamentally cannot provide transaction information.

## Parameters / Member Variables
- `slot`: A TupleTableSlot pointer representing the virtual tuple slot being queried for transaction information

## Dependencies
- Functions called/Symbols referenced:
  - TTS_EMPTY (macro to check if slot is empty)
  - ereport (error reporting function)
- Called from (representative examples):
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md) (at src/backend/executor/execTuples.c:1116)

## Notes and Other Information
- The function includes an assertion that the slot is not empty before attempting to report the error
- The return statement `return false` is included only to silence compiler warnings, as the function will always throw an error before reaching that point
- This is a static function, meaning it's only accessible within the execTuples.c file
- VirtualTupleTableSlots are designed for efficiency when dealing with computed values that don't need persistent storage