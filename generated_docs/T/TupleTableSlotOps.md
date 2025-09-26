# TupleTableSlotOps

## Location
[src/include/executor/tuptable.h:111-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/tuptable.h#L111-L113)

## Overview
TupleTableSlotOps is a function pointer structure that defines the virtual method table (vtable) for different TupleTableSlot implementations, providing polymorphic behavior for slot operations.

## Definition
```c
struct TupleTableSlotOps
{
    /* Minimum size of the slot */
    size_t      base_slot_size;

    /* Initialization. */
    void        (*init) (TupleTableSlot *slot);

    /* Destruction. */
    void        (*release) (TupleTableSlot *slot);

    /* Clear the contents of the slot. */
    void        (*clear) (TupleTableSlot *slot);

    /* Fill up first natts entries of tts_values and tts_isnull arrays */
    void        (*getsomeattrs) (TupleTableSlot *slot, int natts);

    /* Returns value of the given system attribute */
    Datum       (*getsysattr) (TupleTableSlot *slot, int attnum, bool *isnull);

    /* Check if the tuple is created by the current transaction */
    bool        (*is_current_xact_tuple) (TupleTableSlot *slot);

    /* Make the contents of the slot solely depend on the slot */
    void        (*materialize) (TupleTableSlot *slot);

    /* Copy the contents of the source slot into the destination slot */
    void        (*copyslot) (TupleTableSlot *dstslot, TupleTableSlot *srcslot);

    /* Return a heap tuple "owned" by the slot */
    HeapTuple   (*get_heap_tuple) (TupleTableSlot *slot);

    /* Return a minimal tuple "owned" by the slot */
    MinimalTuple (*get_minimal_tuple) (TupleTableSlot *slot);

    /* Return a copy of heap tuple representing the contents of the slot */
    HeapTuple   (*copy_heap_tuple) (TupleTableSlot *slot);

    /* Return a copy of minimal tuple representing the contents of the slot */
    MinimalTuple (*copy_minimal_tuple) (TupleTableSlot *slot);
};
```

## Detailed Description
TupleTableSlotOps implements the Strategy pattern to provide different behaviors for various types of tuple table slots (virtual, heap, minimal, buffer). Each slot type has its own implementation of these operations optimized for its specific storage format and access patterns. This design allows PostgreSQL to efficiently handle different tuple representations while providing a uniform interface.

## Parameters / Member Variables
- `base_slot_size`: Minimum memory size required for the slot implementation
- `init`: Initializes a newly allocated slot of this type
- `release`: Cleans up and deallocates slot-specific resources
- `clear`: Clears slot contents while preserving the tuple descriptor
- `getsomeattrs`: Extracts and materializes attribute values up to natts columns
- `getsysattr`: Retrieves system attributes (like ctid, xmin, xmax) if supported
- `is_current_xact_tuple`: Checks if tuple was created in current transaction
- `materialize`: Ensures slot contents are self-contained and independent
- `copyslot`: Copies source slot contents into destination slot
- `get_heap_tuple`: Returns slot contents as a HeapTuple (slot owns memory)
- `get_minimal_tuple`: Returns slot contents as a MinimalTuple (slot owns memory)
- `copy_heap_tuple`: Creates a copy of slot contents as HeapTuple (caller owns memory)
- `copy_minimal_tuple`: Creates a copy of slot contents as MinimalTuple (caller owns memory)

## Dependencies
- Functions called/Symbols referenced:
  - TupleTableSlot
  - HeapTuple
  - MinimalTuple
  - Datum
- Called from (representative examples):
  - MakeTupleTableSlot
  - ExecAllocTableSlot
  - table_slot_create
  - slot_deform_heap_tuple

## Notes and Other Information
- Different slot implementations include TTSOpsVirtual, TTSOpsHeapTuple, TTSOpsMinimalTuple, and TTSOpsBufferHeapTuple
- The ops structure is referenced as a const pointer in TupleTableSlot.tts_ops
- Some operations may be NULL for slot types that don't support them (e.g., virtual slots don't support get_heap_tuple)
- This abstraction enables efficient tuple processing across different storage formats and execution contexts