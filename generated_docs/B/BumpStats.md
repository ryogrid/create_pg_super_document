# BumpStats

## Location
[src/backend/utils/mmgr/bump.c:688-737](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/bump.c#L688-L737)

## Overview
BumpStats computes and reports memory consumption statistics for a Bump memory context, including total space, free space, and block count information.

## Definition


## Detailed Description
This function analyzes the memory usage of a Bump context by iterating through all blocks and calculating statistics such as total allocated space, free space, and number of blocks. It can optionally format and print these statistics via a callback function and/or accumulate the statistics into a totals counter structure. The function handles both human-readable output formatting and programmatic statistics collection for memory usage monitoring and debugging purposes.

## Parameters / Member Variables
- `context`: The MemoryContext to analyze (cast internally to BumpContext)
- `printfunc`: Optional callback function to receive formatted statistics string
- `passthru`: Pointer passed through to the print function for context
- `totals`: Optional MemoryContextCounters structure to accumulate statistics into
- `print_to_stderr`: Boolean flag controlling whether to print to stderr vs elog

## Dependencies
- Functions called/Symbols referenced:
  - BumpIsValid (context validation)
  - dlist_foreach (block iteration)
  - dlist_container (container extraction)
  - snprintf (string formatting)
- Called from (representative examples):
  - BOGUS_MCTX (via function pointer table)
  - Memory context statistics collection functions

## Notes and Other Information
- Calculates totalspace as block end pointer minus block start pointer
- Calculates freespace as block end pointer minus current free pointer
- Formats statistics string showing total, blocks, free, and used memory
- Can be called for both individual context analysis and system-wide statistics aggregation
- Part of PostgreSQL's memory context debugging and monitoring infrastructure
- Located in src/backend/utils/mmgr/bump.c:688-737