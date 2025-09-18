# AllocSetStats

## Location
src/backend/utils/mmgr/aset.c: 1521 - 1598

## Overview
Computes comprehensive statistics about memory consumption of an AllocSet memory context, including total space, free space, and block counts.

## Definition
```c
void AllocSetStats(MemoryContext context, MemoryStatsPrintFunc printfunc, void *passthru, MemoryContextCounters *totals, bool print_to_stderr)
```

## Detailed Description
AllocSetStats performs a detailed analysis of memory usage within an AllocSet context by traversing all allocated blocks and free lists. It calculates total space (including context header), free space (both in blocks and free chunks), number of blocks, and number of free chunks. The function can optionally format and print human-readable statistics via a callback function, and can accumulate statistics into a totals counter structure. It uses Valgrind annotations to safely access memory chunk headers during the traversal process.

## Parameters / Member Variables
- `context`: The MemoryContext to analyze for statistics
- `printfunc`: Optional callback function to receive formatted statistics string (can be NULL)
- `passthru`: Pointer passed through to the printfunc callback
- `totals`: Optional MemoryContextCounters structure to accumulate statistics (can be NULL)
- `print_to_stderr`: Boolean flag indicating whether to print to stderr (true) or use elog (false)

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetIsValid
  - GetChunkSizeFromFreeListIdx
  - GetFreeListLink
  - MemoryChunkGetValue
  - VALGRIND_MAKE_MEM_DEFINED
  - VALGRIND_MAKE_MEM_NOACCESS
  - snprintf
- Called from (representative examples):
  - BOGUS_MCTX (via function pointer assignment)
  - Referenced in memutils_internal.h

## Notes and Other Information
- Calculates total space including the AllocSetContext header size
- Traverses all memory blocks to compute total and free space
- Examines all freelists (ALLOCSET_NUM_FREELISTS) to count free chunks
- Uses Valgrind annotations for safe memory access during chunk inspection
- Formats statistics as: "X total in Y blocks; Z free (W chunks); V used"
- Can both print statistics immediately and accumulate them for aggregate reporting
- Part of PostgreSQL's memory context system for monitoring and debugging memory usage