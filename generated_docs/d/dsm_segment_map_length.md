# dsm_segment_map_length

## Location
[src/backend/storage/ipc/dsm.c:1105-1122](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm.c#L1105-L1122)

## Overview
Returns the size in bytes of a mapped dynamic shared memory segment.

## Definition
```c
Size dsm_segment_map_length(dsm_segment *seg)
```

## Detailed Description
This function provides access to the mapped size of a dynamic shared memory (DSM) segment. It returns the actual size of the memory region that was mapped into the current process's address space. This information is essential for processes that need to know the boundaries of the shared memory region to avoid accessing memory outside the valid range.

The function serves as a simple accessor to the mapped_size field of the dsm_segment structure, with an assertion to ensure the segment is properly mapped before returning the size information.

## Parameters / Member Variables
- `seg`: Pointer to a dsm_segment structure representing the dynamic shared memory segment

## Dependencies
- Functions called/Symbols referenced:
  - Assert (assertion macro)
  - dsm_segment (structure type)
  - Size (type definition)
- Called from (representative examples):
  - DSM_CREATE_NULL_IF_MAXSEGMENTS (macro in header file)

## Notes and Other Information
- The function includes an assertion that seg->mapped_address is not NULL, ensuring the segment is properly mapped
- Returns a Size type, which is PostgreSQL's standard type for memory sizes
- This is a lightweight accessor function with minimal overhead
- Used to determine the valid memory range for accessing shared memory segments
- Essential for bounds checking when reading from or writing to shared memory regions