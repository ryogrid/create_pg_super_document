# GetMemoryChunkMethodID

## Location
src/backend/utils/mmgr/mcxt.c: 191 - 219

## Overview
Extracts the MemoryContextMethodID from the header that directly precedes a memory chunk pointer, used to identify which memory context method was used to allocate the chunk.

## Definition
```c
static inline MemoryContextMethodID GetMemoryChunkMethodID(const void *pointer)
```

## Detailed Description
This function retrieves the MemoryContextMethodID from the uint64 header that precedes every allocated memory chunk in PostgreSQL's memory management system. The method ID is stored in the lower bits of the header and is masked out using MEMORY_CONTEXT_METHODID_MASK. The function includes basic validation to detect bogus pointers by checking that the pointer is properly aligned (MAXALIGNED). It also uses Valgrind macros to temporarily allow access to the header region for reading, then disallows access again for memory debugging purposes.

## Parameters / Member Variables
- `pointer`: A const void pointer to an allocated memory chunk. Must be properly aligned (MAXALIGNED) and have a valid uint64 header immediately preceding it.

## Dependencies
- Functions called/Symbols referenced:
  - MAXALIGN (for pointer alignment validation)
  - VALGRIND_MAKE_MEM_DEFINED (for memory debugging)
  - VALGRIND_MAKE_MEM_NOACCESS (for memory debugging)
  - MemoryContextMethodID (return type)
  - MEMORY_CONTEXT_METHODID_MASK (for extracting method ID bits)
- Called from (representative examples):
  - MCXT_METHOD (macro)
  - pfree
  - repalloc

## Notes and Other Information
- This is a static inline function for performance efficiency
- The function assumes the pointer points to a valid allocated chunk with a proper header
- Valgrind integration helps detect memory access violations during debugging
- The method ID extraction uses bitwise AND with MEMORY_CONTEXT_METHODID_MASK to isolate the relevant bits from the header
- Part of PostgreSQL's memory context system which tracks allocation methods for each memory chunk