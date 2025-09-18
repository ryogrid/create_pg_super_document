# pfree

## Location
src/backend/utils/mmgr/mcxt.c: 1520 - 1539

## Overview
Releases (deallocates) a previously allocated memory chunk, handling both regular and aligned allocations through the appropriate memory context method.

## Definition


## Detailed Description
The `pfree` function is PostgreSQL's primary interface for deallocating memory that was previously allocated through the memory context system. It provides a unified interface for freeing memory regardless of which memory context or allocation method was used, including regular allocations and aligned allocations.

The function works by extracting the memory chunk header information from the provided pointer to determine the appropriate deallocation method. It then delegates the actual deallocation to the memory context's specific `free_p` method through the `MCXT_METHOD` macro.

For Valgrind-enabled builds, the function provides additional debugging support by notifying Valgrind about the memory deallocation, with special handling for aligned allocations that use redirection headers.

## Parameters / Member Variables
- `pointer`: Pointer to the memory chunk to be deallocated (must have been allocated through the memory context system)

## Dependencies
- Functions called/Symbols referenced:
  - [GetMemoryChunkMethodID](../G/GetMemoryChunkMethodID.md) (when USE_VALGRIND defined)
  - GetMemoryChunkContext (when USE_VALGRIND defined)
  - MCXT_METHOD
  - MCTX_ALIGNED_REDIRECT_ID (when USE_VALGRIND defined)
  - VALGRIND_MEMPOOL_FREE (when USE_VALGRIND defined)
- Called from:
  - Extensively throughout the PostgreSQL codebase wherever memory needs to be freed

## Notes and Other Information
- The function expects the pointer to have been allocated through PostgreSQL's memory context system
- Handles both regular and aligned allocations transparently
- Includes Valgrind integration for memory debugging, with special logic for aligned allocations
- Uses the `MCXT_METHOD` macro to dispatch to the appropriate memory context's free method
- The function does not perform NULL pointer checks - passing NULL will likely cause a crash
- For aligned allocations, the function works with the redirection mechanism established by `MemoryContextAllocAligned`
- Located in src/backend/utils/mmgr/mcxt.c at lines 1520-1539