# tts_buffer_is_current_xact_tuple

## Location
src/backend/executor/execTuples.c: 779 - 801

## Overview
Determines whether a tuple in a buffer-backed heap slot was created by the current transaction by examining its xmin transaction ID.

## Definition
static bool tts_buffer_is_current_xact_tuple(TupleTableSlot *slot)

## Detailed Description
tts_buffer_is_current_xact_tuple checks if a tuple stored in a buffer-backed heap slot was created by the currently executing transaction. This is accomplished by extracting the tuple's xmin value (the transaction ID that created the tuple) using HeapTupleHeaderGetRawXmin and then comparing it with the current transaction ID via TransactionIdIsCurrentTransactionId. The function requires the tuple to be materialized in memory, as it needs direct access to the tuple's header data. If called on a non-materialized slot, it raises an error indicating that storage tuple access is not available in that context.

## Parameters / Member Variables
- : A pointer to the TupleTableSlot containing the buffer-backed heap tuple to examine

## Dependencies
- Functions called/Symbols referenced:
  - BufferHeapTupleTableSlot (cast target type)
  - TTS_EMPTY (slot state check macro)
  - HeapTupleHeaderGetRawXmin (transaction ID extraction function)
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md) (transaction comparison function)
- Called from (representative examples):
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md) (as part of vtable operations)

## Notes and Other Information
- This is a static function, accessible only within execTuples.c
- Part of the tuple table slot operations vtable pattern for buffer-backed heap slots
- Includes assertions to prevent access to empty slots
- Requires tuple materialization - raises ERRCODE_FEATURE_NOT_SUPPORTED error for non-materialized tuples
- Used for transaction visibility and concurrency control decisions
- Returns a boolean indicating whether the tuple belongs to the current transaction
- The xmin field represents the transaction ID that inserted/created the tuple
- Critical for MVCC (Multi-Version Concurrency Control) implementation in PostgreSQL