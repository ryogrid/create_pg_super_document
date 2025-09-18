# repalloc

## Location
src/backend/utils/mmgr/mcxt.c: 1540 - 1580

## Overview
Resizes a previously allocated memory chunk to a new size, preserving existing data and handling both regular and aligned allocations.

## Definition


## Detailed Description
The `repalloc` function provides memory reallocation functionality within PostgreSQL's memory context system. It adjusts the size of a previously allocated memory chunk, preserving the existing data up to the minimum of the old and new sizes. The function works with memory allocated through any memory context allocation method, including regular and aligned allocations.

The implementation delegates the actual reallocation to the appropriate memory context method through the `MCXT_METHOD` macro, ensuring that each memory context type can handle reallocation in the most efficient way possible. The function includes optimization considerations, deliberately offloading allocation failure handling to the underlying method implementations to enable compiler optimizations like sibling call optimization.

For Valgrind-enabled builds, the function provides debugging support by notifying Valgrind about the memory size change, with special handling for aligned allocations that use redirection headers.

## Parameters / Member Variables
- `pointer`: Pointer to the previously allocated memory chunk to be resized
- `size`: The new size in bytes for the memory chunk

## Dependencies
- Functions called/Symbols referenced:
  - [GetMemoryChunkMethodID](../G/GetMemoryChunkMethodID.md) (when USE_VALGRIND defined)
  - GetMemoryChunkContext (when USE_ASSERT_CHECKING or USE_VALGRIND defined)
  - AssertNotInCriticalSection
  - MCXT_METHOD
  - MCTX_ALIGNED_REDIRECT_ID (when USE_VALGRIND defined)
  - VALGRIND_MEMPOOL_CHANGE (when USE_VALGRIND defined)
- Called from (representative examples):
  - [brin_copy_tuple](../b/brin_copy_tuple.md)
  - [add_reloption](../a/add_reloption.md)
  - [tidstore_iter_extract_tids](../t/tidstore_iter_extract_tids.md)
  - [GinFormTuple](../G/GinFormTuple.md)
  - [ExprEvalPushStep](../E/ExprEvalPushStep.md)
  - [SPI_repalloc](../S/SPI_repalloc.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [enlarge_list](../e/enlarge_list.md)
  - (extensively used throughout PostgreSQL codebase)

## Notes and Other Information
- The function expects the pointer to have been allocated through PostgreSQL's memory context system
- Preserves existing data up to the minimum of old and new sizes
- Handles both regular and aligned allocations transparently
- Includes debug assertions to ensure the memory context is valid and not in a critical section
- The function assumes the memory context's `isReset` flag is already false
- Designed for compiler optimization - avoid adding code after the `MCXT_METHOD` call
- For Valgrind builds, includes special handling for aligned allocations using redirection headers
- Returns a potentially different pointer - callers must use the returned value
- Located in src/backend/utils/mmgr/mcxt.c at lines 1540-1580