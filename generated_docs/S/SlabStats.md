# SlabStats

## Location
src/backend/utils/mmgr/slab.c: 929 - 996

## Overview
SlabStats computes and reports memory consumption statistics for a Slab memory context, providing detailed information about blocks, chunks, and memory usage.

## Definition


## Detailed Description
SlabStats walks through all blocks in a Slab memory context to collect comprehensive statistics about memory usage. It calculates the total space consumed, free space available, number of blocks, and free chunks. The function can either print human-readable statistics via a provided callback function or accumulate the statistics into a totals counter structure for aggregation across multiple contexts.

The function examines both empty blocks (stored in the emptyblocks list) and active blocks (organized in blocklist arrays by free chunk count). For each block, it tracks the block size, number of free chunks, and calculates free space based on the full chunk size multiplied by the number of free chunks.

## Parameters / Member Variables
- : The MemoryContext to analyze (cast internally to SlabContext)
- : Optional callback function to receive formatted statistics string
- : Opaque pointer passed through to printfunc callback
- : Optional MemoryContextCounters structure to accumulate statistics into
- : Boolean flag controlling whether stats are printed to stderr or logged via elog

## Dependencies
- Functions called/Symbols referenced:
  - SlabIsValid
  - Slab_CONTEXT_HDRSZ
  - [dclist_count](../d/dclist_count.md)
  - dlist_foreach
  - dlist_container
  - snprintf
- Called from (representative examples):
  - Memory context management functions via BOGUS_MCTX
  - Internal memory utility functions

## Notes and Other Information
The function includes context header size in total space calculations and provides detailed breakdown including empty blocks count. The formatted output string includes total space, number of blocks, empty blocks count, free space with chunk count, and used space. This function is part of PostgreSQL's memory management debugging and monitoring infrastructure, allowing administrators and developers to understand Slab allocator behavior and memory consumption patterns.