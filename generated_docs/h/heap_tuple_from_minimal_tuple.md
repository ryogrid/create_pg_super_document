# heap_tuple_from_minimal_tuple

## Location
[src/backend/access/common/heaptuple.c:1554-1575](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/heaptuple.c#L1554-L1575)

## Overview
Creates a full HeapTuple structure from a MinimalTuple by adding a HeapTupleData header and system columns filled with default values.

## Definition
```c
HeapTuple heap_tuple_from_minimal_tuple(MinimalTuple mtup)
```

## Detailed Description
The `heap_tuple_from_minimal_tuple` function converts a MinimalTuple into a complete HeapTuple structure. This is useful when code that expects a full HeapTuple needs to work with data that's currently in minimal tuple format. The function:

1. Calculates the total size needed (MinimalTuple size + MINIMAL_TUPLE_OFFSET for system columns)
2. Allocates memory for both the HeapTuple structure and tuple data as a single block
3. Initializes the HeapTuple header with default values:
   - Sets invalid item pointer (t_self)
   - Sets invalid table OID (t_tableOid)
4. Sets up the data pointer to point to the allocated tuple data area
5. Copies the MinimalTuple data to the appropriate offset in the new structure
6. Zeros out the system column area (before t_infomask2 in HeapTupleHeaderData)

The result includes a complete HeapTupleData header with system columns set to zero, making it compatible with all PostgreSQL subsystems that expect full HeapTuples.

## Parameters / Member Variables
- `mtup`: Pointer to the source MinimalTuple to be converted. Must be a valid MinimalTuple structure.

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - memcpy
  - memset
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md)
  - MINIMAL_TUPLE_OFFSET
  - HEAPTUPLESIZE
  - InvalidOid
- Called from (representative examples):
  - [tts_minimal_copy_heap_tuple](../t/tts_minimal_copy_heap_tuple.md)
  - HeapTupleClearHeapOnly

## Notes and Other Information
- Creates a complete HeapTuple with system columns initialized to default values
- The HeapTuple struct and data are allocated as a single memory block for efficiency
- System columns (t_xmin, t_xmax, t_cmin, t_cmax, etc.) are zeroed out
- The t_self field is set to invalid, indicating this tuple is not stored on disk
- The t_tableOid is set to InvalidOid since minimal tuples don't carry table information
- Used primarily in executor tuple table slots for converting between minimal and heap tuple formats
- The resulting HeapTuple must be freed using `heap_freetuple()` when no longer needed
- More expensive than working directly with MinimalTuples due to additional header overhead
- Essential for interfacing between subsystems that use different tuple formats