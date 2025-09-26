# ExecCopySlotMinimalTuple

## Location
[src/include/executor/tuptable.h:492-508](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/tuptable.h#L492-L508)

## Overview
Returns a MinimalTuple allocated in the caller's memory context by copying the tuple data from a TupleTableSlot.

## Definition
```c
static inline MinimalTuple
ExecCopySlotMinimalTuple(TupleTableSlot *slot)
```

## Detailed Description
ExecCopySlotMinimalTuple creates a MinimalTuple copy of the tuple stored in a TupleTableSlot, allocating the new tuple in the caller's current memory context. MinimalTuple is a compact tuple representation used for memory-efficient storage of tuples, particularly in scenarios like sorting, hashing, and temporary storage where space optimization is important.

The function delegates to the slot's type-specific copy_minimal_tuple operation through the tts_ops function pointer table, allowing different slot types to implement their own copying logic while providing a uniform interface. This abstraction enables efficient conversion from any slot type to the minimal tuple format.

## Parameters / Member Variables
- `slot`: Pointer to the TupleTableSlot containing the tuple to copy

## Dependencies
- Functions called/Symbols referenced:
  - [TupleTableSlot](../T/TupleTableSlot.md) (struct type)
  - tts_ops->copy_minimal_tuple (function pointer)
- Called from (representative examples):
  - [LookupTupleHashEntry_internal](../L/LookupTupleHashEntry_internal.md)
  - [tts_minimal_copyslot](../t/tts_minimal_copyslot.md)
  - [cache_lookup](../c/cache_lookup.md)
  - [cache_store_tuple](../c/cache_store_tuple.md)
  - [tuplesort_puttupleslot](../t/tuplesort_puttupleslot.md)
  - [tuplestore_puttupleslot](../t/tuplestore_puttupleslot.md)

## Notes and Other Information
- The returned MinimalTuple is allocated in the caller's current memory context
- MinimalTuple is a more compact representation compared to HeapTuple, making it ideal for memory-constrained operations
- Commonly used in sorting, hashing, and caching operations where memory efficiency is critical
- Unlike ExecCopySlotHeapTuple, this function doesn't include an explicit empty slot assertion, relying on the underlying implementation
- Part of the TupleTableSlot abstraction layer that provides uniform access to different tuple storage formats
- Essential for operations like tuple stores, sorts, and hash tables that need to store many tuples efficiently
- The minimal tuple format excludes some metadata present in HeapTuple to save space
- Used extensively in executor nodes that need to cache or temporarily store large numbers of tuples