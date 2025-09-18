# dsa_allocate_extended

## Location
[src/backend/utils/mmgr/dsa.c:671-825](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L671-L825)

## Overview
Allocates memory in a dynamic shared memory area and returns a portable dsa_pointer that can be shared across processes.

## Definition


## Detailed Description
This function allocates memory of the specified size within a dynamic shared memory (DSA) area. Unlike traditional memory allocation functions, it returns a  which is a portable reference that can be passed to other processes and converted to a local pointer using . The function supports various allocation strategies based on the provided flags:

For very large allocations (larger than the largest size class), the function bypasses the normal pooled allocation system and directly requests page runs from the free page manager. For smaller allocations, it maps the requested size to an appropriate size class and allocates from the corresponding object pool.

The allocation process involves acquiring appropriate locks, finding or creating segments with sufficient space, and initializing the necessary metadata structures including spans and page maps.

## Parameters / Member Variables
- : Pointer to the DSA area from which to allocate memory
- : Number of bytes to allocate (must be greater than 0)
- : Bitmap controlling allocation behavior, constructed from:
  - : Allows allocations >= 1GB
  - : Returns InvalidDsaPointer on failure instead of raising ERROR
  - : Zero-initializes the allocated memory

## Dependencies
- Functions called/Symbols referenced:
  - , 
  - , 
  - 
  - , 
  - 
  - , 
  - , 
- Called from:
  -  (macro wrapper)
  -  (macro wrapper)
  - 
  -  (in dshash.c)
  - 

## Notes and Other Information
- Returns  on allocation failure when  is set
- Large allocations (> largest size class) use a special span management system
- Small allocations use a size class mapping system with lookup tables and binary search
- Thread-safe through appropriate locking mechanisms
- Memory contents are indeterminate unless  flag is used
- The function enforces size limits and validates allocation requests for safety