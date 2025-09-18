# GetMemoryChunkHeader

## Location
src/backend/utils/mmgr/mcxt.c: 220 - 256

## Overview
Retrieves the complete uint64 header that directly precedes a memory chunk pointer, providing access to the full header information including method ID and other metadata.

## Definition
```c
static inline uint64 GetMemoryChunkHeader(const void *pointer)
```

## Detailed Description
This function returns the complete uint64 header that precedes every allocated memory chunk in PostgreSQL's memory management system. Unlike GetMemoryChunkMethodID which extracts only the method ID portion, this function returns the entire header value. The function is designed to be used after GetMemoryChunkMethodID has already validated the pointer, so it omits error checking for performance. It uses Valgrind macros to temporarily allow access to the header region for reading, then disallows access again for memory debugging purposes.

## Parameters / Member Variables
- `pointer`: A const void pointer to an allocated memory chunk. Should be a valid pointer that has already been validated by GetMemoryChunkMethodID.

## Dependencies
- Functions called/Symbols referenced:
  - VALGRIND_MAKE_MEM_DEFINED (for memory debugging)
  - VALGRIND_MAKE_MEM_NOACCESS (for memory debugging)
- Called from (representative examples):
  - BogusFree
  - BogusRealloc
  - BogusGetChunkContext
  - BogusGetChunkSpace

## Notes and Other Information
- This is a static inline function for performance efficiency
- Assumes the pointer has already been validated, so no error checking is performed
- Used primarily by the 'Bogus' family of functions which handle error reporting for invalid memory operations
- Part of PostgreSQL's memory context system architecture
- The header contains various metadata about the memory chunk including method ID and size information
- Valgrind integration helps detect memory access violations during debugging