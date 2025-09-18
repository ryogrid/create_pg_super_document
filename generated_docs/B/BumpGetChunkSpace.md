# BumpGetChunkSpace

## Location
src/backend/utils/mmgr/bump.c: 649 - 659

## Overview
BumpGetChunkSpace is a stub function in the Bump memory allocator that deliberately throws an error as this functionality is not supported by the bump allocator design.

## Definition


## Detailed Description
This function is part of the MemoryContext interface but is intentionally unimplemented in the Bump allocator. The Bump allocator is designed as a simple, fast allocator that only supports allocation and reset operations, but does not track individual chunk sizes or provide chunk introspection capabilities. When called, it immediately raises an ERROR with the message that "GetMemoryChunkSpace is not supported by the bump memory allocator".

## Parameters / Member Variables
- : A pointer to a memory chunk (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - elog (for error reporting)
- Called from (representative examples):
  - BOGUS_MCTX (via function pointer table)
  - Memory context interface functions

## Notes and Other Information
- This is a deliberate design choice - the Bump allocator trades functionality for speed and simplicity
- The function returns 0 to keep the compiler quiet, but this line is never reached due to the ERROR
- Part of the standard MemoryContext interface but not meaningful for bump allocation strategy
- Located in src/backend/utils/mmgr/bump.c:649-659