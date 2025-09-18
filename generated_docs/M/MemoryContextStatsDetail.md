# MemoryContextStatsDetail

## Location
src/backend/utils/mmgr/mcxt.c: 829 - 875

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
  - MemoryContextCounters (structure for accumulating statistics)
  - MemoryContextStatsInternal (internal recursive statistics gathering)
  - LOG_SERVER_ONLY (logging level constant)
  - errhidestmt (error reporting function)
  - errhidecontext (error reporting function)
- Called from (representative examples):
  - MemoryContextStats (simplified wrapper)
  - ProcessLogMemoryContextInterrupt (signal-based debugging)

## Notes and Other Information
- Uses LOG_SERVER_ONLY when logging to prevent memory context data from reaching clients
- Avoids buffering large amounts of data to prevent OOM conditions in backends with many contexts
- Displays grand totals including total space, number of blocks, free space, free chunks, and used space
- The max_level and max_children parameters help control output volume for large context hierarchies
- Commonly used in debugging scenarios and automated monitoring systems