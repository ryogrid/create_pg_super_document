# heap_copy_minimal_tuple

## Location
[src/backend/access/common/heaptuple.c:1535-1553](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/heaptuple.c#L1535-L1553)

## Overview
Creates a copy of an existing MinimalTuple by allocating new memory and performing a byte-for-byte copy of the tuple data.

## Definition
```c
MinimalTuple heap_copy_minimal_tuple(MinimalTuple mtup)
```

## Detailed Description
The `heap_copy_minimal_tuple` function creates a complete copy of a MinimalTuple structure. It allocates memory for the new tuple using `palloc()` with the same size as the original tuple, then performs a `memcpy()` to copy all the data from the source tuple to the new tuple. This creates an independent copy that can be modified or freed without affecting the original.

The function is straightforward:
1. Allocates memory equal to the size of the source tuple (mtup->t_len)
2. Copies all bytes from the source to the destination using memcpy
3. Returns the new independent copy

The result is allocated in the current memory context and must be freed by the caller when no longer needed.

## Parameters / Member Variables
- `mtup`: Pointer to the source MinimalTuple to be copied. Must be a valid MinimalTuple structure.

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - memcpy
- Called from (representative examples):
  - [tts_minimal_materialize](../t/tts_minimal_materialize.md)
  - [tts_minimal_copy_minimal_tuple](../t/tts_minimal_copy_minimal_tuple.md)
  - [gm_readnext_tuple](../g/gm_readnext_tuple.md)
  - [tuplesort_gettupleslot](../t/tuplesort_gettupleslot.md)
  - [tuplestore_gettupleslot](../t/tuplestore_gettupleslot.md)
  - HeapTupleClearHeapOnly

## Notes and Other Information
- Creates a complete independent copy of the source MinimalTuple
- The copy is allocated in the current memory context
- Commonly used in tuple table slots and sorting operations where tuple copies are needed
- The resulting copy must be freed using `heap_free_minimal_tuple()` when no longer needed
- Used extensively in executor operations where tuples need to be preserved across operations
- More efficient than constructing a new tuple from values since it performs a simple memory copy
- Essential for operations that need to maintain tuple data beyond the lifetime of the original