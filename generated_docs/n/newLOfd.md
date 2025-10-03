# newLOfd

## Location
[src/backend/libpq/be-fsstubs.c:675-715](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-fsstubs.c#L675-L715)

## Overview
Allocates a new file descriptor slot for large object operations by finding a free slot in the cookies array or expanding the array as needed.

## Definition
```c
static int newLOfd(void)
```

## Detailed Description
newLOfd is a static utility function that manages the allocation of file descriptor slots for large objects. It implements a dynamic array management strategy:

1. Sets the `lo_cleanup_needed` flag to indicate large object operations are active
2. Creates the filesystem memory context (`fscxt`) if it doesn't exist
3. Searches for an available slot in the existing `cookies` array
4. If no free slot is found, expands the array:
   - Initial allocation: Creates a 64-element array
   - Subsequent expansions: Doubles the current array size
5. Returns the index of the allocated slot

The function uses a zero-initialized allocation strategy to ensure that new slots are properly initialized as NULL, and employs `repalloc0_array` for safe array expansion that preserves existing data while zeroing new elements.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - ALLOCSET_DEFAULT_SIZES
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - repalloc0_array
  - [LargeObjectDesc](../L/LargeObjectDesc.md) (struct type)
- Called from (representative examples):
  - [be_lo_open](../b/be_lo_open.md) (src/backend/libpq/be-fsstubs.c:105)

## Notes and Other Information
- Static function with file-local scope, used internally by large object operations
- Uses exponential growth strategy (doubling) for array expansion to minimize reallocations
- Initial array size of 64 elements provides reasonable starting capacity for most workloads
- Memory allocation occurs in the `fscxt` context, ensuring proper cleanup during transaction end
- Sets global `lo_cleanup_needed` flag to ensure end-of-transaction cleanup occurs
- Zero-initialization of new array elements ensures NULL pointers for unused slots
- Thread-safe within the context of PostgreSQL's single-threaded backend model