# MemoryContextStatsInternal

## Location
[src/backend/utils/mmgr/mcxt.c:876-972](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L876-L972)

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
  - [MemoryContextCounters](MemoryContextCounters.md) (statistics accumulation structure)
  - MemoryContextIsValid (context validation)
  - [MemoryContextStatsPrint](MemoryContextStatsPrint.md) (context-specific statistics printing callback)
  - [stack_is_too_deep](../s/stack_is_too_deep.md) (stack overflow protection)
  - [MemoryContextTraverseNext](MemoryContextTraverseNext.md) (non-recursive context traversal)
  - LOG_SERVER_ONLY (logging level for ereport)
  - [errhidestmt](../e/errhidestmt.md)/errhidecontext (error reporting functions)
- Called from (representative examples):
  - [MemoryContextStatsDetail](MemoryContextStatsDetail.md) (main entry point)
  - [MemoryContextStatsInternal](MemoryContextStatsInternal.md) (recursive self-calls)

## Notes and Other Information
- Static function, only accessible within mcxt.c
- Implements stack overflow protection via stack_is_too_deep() checks
- Uses context->methods->stats callback for context-specific statistics gathering
- Switches to non-recursive mode when limits are exceeded to avoid deep recursion
- Provides detailed indentation in output to show context hierarchy levels
- Accumulates statistics in totals parameter when provided (can be NULL)
- Uses MemoryContextTraverseNext for safe non-recursive traversal of remaining contexts
- Critical for debugging memory usage patterns and detecting memory leaks in complex applications

## Simplified Source

```c
// Simplified version of MemoryContextStatsInternal
static void MemoryContextStatsInternal(MemoryContext context, int level,
                                       int max_level, int max_children,
                                       MemoryContextCounters *totals,
                                       bool print_to_stderr) {
    MemoryContext child;
    int child_count = 0;

    // Validate the context
    Assert(MemoryContextIsValid(context));

    // Get statistics for the current context
    context->methods->stats(context, MemoryContextStatsPrint, &level, totals, print_to_stderr);

    // Process child contexts if within depth and stack limits
    child = context->firstchild;
    if (level < max_level && !stack_is_too_deep()) {
        // Recursively process up to max_children
        while (child != NULL && child_count < max_children) {
            MemoryContextStatsInternal(child, level + 1, max_level, max_children,
                                       totals, print_to_stderr);
            child = child->nextchild;
            child_count++;
        }
    }

    // If there are remaining children, summarize them without recursion
    if (child != NULL) {
        MemoryContextCounters remaining_totals;
        memset(&remaining_totals, 0, sizeof(remaining_totals));

        // Traverse remaining children and accumulate stats
        child_count = 0;
        while (child != NULL) {
            child->methods->stats(child, NULL, NULL, &remaining_totals, false);
            child_count++;
            child = MemoryContextTraverseNext(child, context);
        }

        // Print summary of remaining children
        if (print_to_stderr) {
            // Print with proper indentation
            for (int i = 0; i <= level; i++) {
                fprintf(stderr, "  ");
            }
            fprintf(stderr, "%d more child contexts containing %zu total in %zu blocks\n",
                    child_count, remaining_totals.totalspace, remaining_totals.nblocks);
        } else {
            // Log summary information
            ereport(LOG_SERVER_ONLY,
                    (errmsg_internal("level: %d; %d more child contexts", level, child_count)));
        }

        // Add to grand totals if requested
        if (totals) {
            totals->nblocks += remaining_totals.nblocks;
            totals->freechunks += remaining_totals.freechunks;
            totals->totalspace += remaining_totals.totalspace;
            totals->freespace += remaining_totals.freespace;
        }
    }
}
```

Key simplifications made:
- Combined variable declarations and simplified loop structure
- Removed complex error reporting details, keeping core logging logic
- Consolidated the fprintf and ereport output formatting for clarity
- Simplified the statistics accumulation logic while preserving functionality
- Added clearer comments explaining each major section
- Maintained the essential recursive vs non-recursive processing logic
- Preserved all critical safety checks (stack depth, context validation)