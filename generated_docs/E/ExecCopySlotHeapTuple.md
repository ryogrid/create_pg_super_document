# ExecCopySlotHeapTuple

## Location
[src/include/executor/tuptable.h:481-491](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/tuptable.h#L481-L491)

## Overview
Returns a HeapTuple allocated in the caller's memory context by copying the tuple data from a TupleTableSlot.

## Definition
```c
static inline HeapTuple
ExecCopySlotHeapTuple(TupleTableSlot *slot)
```

## Detailed Description
ExecCopySlotHeapTuple creates a HeapTuple copy of the tuple stored in a TupleTableSlot, allocating the new tuple in the caller's current memory context. This function is essential when code needs a HeapTuple representation that will persist beyond the lifetime of the original slot's storage context.

The function includes an assertion to ensure the slot is not empty before attempting the copy operation. It delegates to the slot's type-specific copy_heap_tuple operation through the tts_ops function pointer table, allowing different slot types to implement their own copying logic while providing a uniform interface.

## Parameters / Member Variables
- `slot`: Pointer to the TupleTableSlot containing the tuple to copy (must not be empty)

## Dependencies
- Functions called/Symbols referenced:
  - TTS_EMPTY (macro for checking if slot is empty)
  - TupleTableSlot (struct type)
  - tts_ops->copy_heap_tuple (function pointer)
  - MinimalTuple (type referenced in related code)
- Called from (representative examples):
  - acquire_sample_rows
  - tts_heap_copyslot
  - tts_buffer_heap_copyslot
  - agg_retrieve_direct
  - reorderqueue_push
  - setop_retrieve_direct
  - ExecScanSubPlan
  - ExecSetParamPlan
  - spi_printtup

## Notes and Other Information
- The returned HeapTuple is allocated in the caller's current memory context
- Includes a runtime assertion to prevent copying from empty slots
- Used when code specifically needs a HeapTuple representation rather than working with the slot abstraction
- Critical for interfacing with code that expects traditional HeapTuple structures
- Part of the TupleTableSlot abstraction layer that provides uniform access to different tuple storage formats
- The copying operation creates an independent copy that doesn't depend on the original slot's storage
- Commonly used in aggregate operations, sampling, and result set handling where persistent HeapTuple structures are required