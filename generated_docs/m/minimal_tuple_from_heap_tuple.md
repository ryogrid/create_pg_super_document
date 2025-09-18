# minimal_tuple_from_heap_tuple

## Location
[src/backend/access/common/heaptuple.c:1576-1593](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/heaptuple.c#L1576-L1593)

## Overview
Creates a MinimalTuple by copying data from a HeapTuple, removing the HeapTuple-specific overhead to produce a more compact representation.

## Definition
```c
MinimalTuple minimal_tuple_from_heap_tuple(HeapTuple htup)
```

## Detailed Description
This function converts a HeapTuple to a MinimalTuple by copying only the essential tuple data while stripping away the HeapTuple-specific header information. The conversion creates a more compact representation by removing MINIMAL_TUPLE_OFFSET bytes from the beginning of the HeapTuple's data, which contains HeapTuple-specific metadata that is not needed in a MinimalTuple.

The function allocates memory for the new MinimalTuple in the current memory context and performs a direct memory copy of the relevant data portion. The resulting MinimalTuple has its length field set appropriately to reflect the reduced size.

## Parameters / Member Variables
- `htup`: The source HeapTuple to convert. Must have a length greater than MINIMAL_TUPLE_OFFSET to ensure valid data exists after stripping the offset.

## Dependencies
- Functions called/Symbols referenced:
  - MinimalTuple (type)
  - MINIMAL_TUPLE_OFFSET (constant)
  - [palloc](../p/palloc.md) (memory allocation)
  - memcpy (memory copy)
  - Assert (assertion macro)

- Called from (representative examples):
  - [tts_heap_copy_minimal_tuple](../t/tts_heap_copy_minimal_tuple.md)
  - [tts_buffer_heap_copy_minimal_tuple](../t/tts_buffer_heap_copy_minimal_tuple.md)
  - [copytup_heap](../c/copytup_heap.md)
  - HeapTupleClearHeapOnly

## Notes and Other Information
- The function assumes that the input HeapTuple has sufficient data beyond the MINIMAL_TUPLE_OFFSET
- Memory allocation is done in the current memory context, so the caller is responsible for managing the memory context appropriately
- The conversion is a one-way operation - information lost during the conversion cannot be recovered
- Used primarily in scenarios where space efficiency is important, such as tuple sorting and temporary storage