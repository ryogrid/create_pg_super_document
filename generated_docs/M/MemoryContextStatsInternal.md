# MemoryContextStatsInternal

## Location
src/backend/utils/mmgr/mcxt.c: 876 - 972

## Overview
MemoryContextStatsInternal is the core recursive function that implements memory context statistics collection and reporting with intelligent depth control and stack overflow protection.

## Definition
```c
static void MemoryContextStatsInternal(MemoryContext context, int level,
                                       int max_level, int max_children,
                                       MemoryContextCounters *totals,
                                       bool print_to_stderr)
```

## Detailed Description
This static function performs the actual recursive traversal of memory context hierarchies to collect and report statistics. It implements several important safety and performance optimizations: stack depth monitoring to prevent stack overflow, configurable limits on recursion depth and child context display, and intelligent summarization when limits are exceeded. The function examines the current context using its methods->stats callback, then recursively processes child contexts up to the specified limits. When limits are exceeded, it switches to a non-recursive traversal mode to gather summary statistics for the remaining contexts.

## Parameters / Member Variables
- `context`: The memory context to examine and report statistics for
- `level`: Current recursion depth level in the context hierarchy
- `max_level`: Maximum recursion depth allowed before switching to summary mode
- `max_children`: Maximum number of child contexts to process individually per level
- `totals`: Pointer to MemoryContextCounters structure for accumulating grand totals across all contexts
- `print_to_stderr`: If true, output via fprintf to stderr; if false, use ereport logging

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextCounters (statistics accumulation structure)
  - MemoryContextIsValid (context validation)
  - MemoryContextStatsPrint (context-specific statistics printing callback)
  - stack_is_too_deep (stack overflow protection)
  - MemoryContextTraverseNext (non-recursive context traversal)
  - LOG_SERVER_ONLY (logging level for ereport)
  - errhidestmt/errhidecontext (error reporting functions)
- Called from (representative examples):
  - MemoryContextStatsDetail (main entry point)
  - MemoryContextStatsInternal (recursive self-calls)

## Notes and Other Information
- Static function, only accessible within mcxt.c
- Implements stack overflow protection via stack_is_too_deep() checks
- Uses context->methods->stats callback for context-specific statistics gathering
- Switches to non-recursive mode when limits are exceeded to avoid deep recursion
- Provides detailed indentation in output to show context hierarchy levels
- Accumulates statistics in totals parameter when provided (can be NULL)
- Uses MemoryContextTraverseNext for safe non-recursive traversal of remaining contexts
- Critical for debugging memory usage patterns and detecting memory leaks in complex applications