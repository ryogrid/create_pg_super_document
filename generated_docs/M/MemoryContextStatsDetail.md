# MemoryContextStatsDetail

## Location
[src/backend/utils/mmgr/mcxt.c:829-875](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L829-L875)

## Overview
MemoryContextStatsDetail provides configurable memory context statistics reporting with control over output depth and format, supporting both stderr and logging system output.

## Definition
```c
void MemoryContextStatsDetail(MemoryContext context,
                              int max_level, int max_children,
                              bool print_to_stderr)
```

## Detailed Description
This function serves as the primary entry point for detailed memory context statistics reporting with customizable parameters. It allows fine-grained control over the output format and depth of context hierarchy traversal. The function accumulates grand totals across all contexts and presents a comprehensive summary. When using the logging system instead of stderr, it employs LOG_SERVER_ONLY to prevent sensitive memory information from being sent to connected clients. The implementation avoids buffering all context information to prevent potential out-of-memory conditions when dealing with large numbers of memory contexts.

## Parameters / Member Variables
- `context`: The root memory context to analyze, including its descendant hierarchy
- `max_level`: Maximum depth of context hierarchy to traverse (prevents excessive output)
- `max_children`: Maximum number of child contexts to display per level
- `print_to_stderr`: If true, output goes to stderr via fprintf; if false, uses ereport() logging system

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextCounters](MemoryContextCounters.md) (structure for accumulating statistics)
  - [MemoryContextStatsInternal](MemoryContextStatsInternal.md) (internal recursive statistics gathering)
  - LOG_SERVER_ONLY (logging level constant)
  - [errhidestmt](../e/errhidestmt.md) (error reporting function)
  - [errhidecontext](../e/errhidecontext.md) (error reporting function)
- Called from (representative examples):
  - [MemoryContextStats](MemoryContextStats.md) (simplified wrapper)
  - [ProcessLogMemoryContextInterrupt](../P/ProcessLogMemoryContextInterrupt.md) (signal-based debugging)

## Notes and Other Information
- Uses LOG_SERVER_ONLY when logging to prevent memory context data from reaching clients
- Avoids buffering large amounts of data to prevent OOM conditions in backends with many contexts
- Displays grand totals including total space, number of blocks, free space, free chunks, and used space
- The max_level and max_children parameters help control output volume for large context hierarchies
- Commonly used in debugging scenarios and automated monitoring systems

## Simplified Source

```c
// Simplified version of MemoryContextStatsDetail
void MemoryContextStatsDetail(MemoryContext context,
                              int max_level, int max_children,
                              bool print_to_stderr)
{
    // Initialize counters to accumulate statistics
    MemoryContextCounters grand_totals;
    memset(&grand_totals, 0, sizeof(grand_totals));

    // Recursively gather statistics from context hierarchy
    MemoryContextStatsInternal(context, 0, max_level, max_children,
                               &grand_totals, print_to_stderr);

    // Output grand totals in appropriate format
    if (print_to_stderr) {
        // Direct stderr output for debugging
        fprintf(stderr,
                "Grand total: %zu bytes in %zu blocks; %zu free (%zu chunks); %zu used\n",
                grand_totals.totalspace, grand_totals.nblocks,
                grand_totals.freespace, grand_totals.freechunks,
                grand_totals.totalspace - grand_totals.freespace);
    } else {
        // Use logging system with server-only level
        ereport(LOG_SERVER_ONLY,
                (errhidestmt(true),
                 errhidecontext(true),
                 errmsg_internal("Grand total: %zu bytes in %zu blocks; %zu free (%zu chunks); %zu used",
                                 grand_totals.totalspace, grand_totals.nblocks,
                                 grand_totals.freespace, grand_totals.freechunks,
                                 grand_totals.totalspace - grand_totals.freespace)));
    }
}
```

Key simplifications made:
- Removed detailed comments about OOM prevention reasoning (kept essential logic)
- Consolidated the output formatting logic while preserving both paths
- Added brief inline comments to explain each major step
- Maintained the essential algorithm: initialize counters, gather stats, output totals
- Preserved the dual output mechanism (stderr vs logging system)
- Kept the LOG_SERVER_ONLY security consideration