# dsa_free

## Location
[src/backend/utils/mmgr/dsa.c:826-941](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L826-L941)

## Overview
Frees memory that was previously allocated with dsa_allocate or dsa_allocate_extended within a dynamic shared memory area.

## Definition

```c
void
dsa_free(dsa_area *area, dsa_pointer dp)
```
## Detailed Description
This function deallocates memory within a DSA area that was previously allocated using  or . The function handles two different types of allocations:

1. **Large allocations** (DSA_SCLASS_SPAN_LARGE): These are freed by returning pages directly to the free page manager and freeing the associated span object. This involves unlinking the span from its pool and recursively calling  on the span pointer itself.

2. **Regular allocations**: These are freed by adding the object back to the span's freelist and potentially moving the span to a different fullness class or destroying the entire superblock if it becomes completely empty.

The function maintains proper memory management by tracking object allocation state through span metadata, managing fullness classes to optimize allocation performance, and preventing memory fragmentation through intelligent superblock destruction policies.

## Parameters / Member Variables
- : Pointer to the DSA area containing the memory to be freed
- : The dsa_pointer representing the memory block to free (must be a valid pointer returned by a previous allocation)

## Dependencies
- Functions called/Symbols referenced:
  - 
  - , , 
  - 
  - , 
  - , 
  - 
  - , 
  -  (recursive call for span objects)
- Called from:
  - Various executor functions (, , etc.)
  - Hash table operations (, etc.)
  - , 
  - , 
  - 
  - Radix tree operations (, , etc.)
  -  (on error cleanup)

## Notes and Other Information
- The function automatically detects whether the memory being freed is a large allocation or regular allocation
- For debugging builds with CLOBBER_FREED_MEMORY defined, freed memory is overwritten with 0x7f bytes
- Thread-safe through appropriate locking mechanisms using DSA area locks and size class locks
- Implements intelligent superblock management to prevent memory fragmentation and hysteresis
- The active span in fullness class 1 is preserved even when empty to avoid allocation/deallocation thrashing
- Large object spans are handled specially with direct page manager interaction
- Recursive freeing is used for large allocation span objects