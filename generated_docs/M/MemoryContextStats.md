# MemoryContextStats

## Location
[src/backend/utils/mmgr/mcxt.c:814-828](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L814-L828)

## Overview
MemoryContextStats is a debugging utility function that prints comprehensive statistics about a specified memory context and all its descendant contexts to stderr.

## Definition
```c
void MemoryContextStats(MemoryContext context)
```

## Detailed Description
This function serves as a convenient wrapper around MemoryContextStatsDetail, providing a simple interface for obtaining memory context statistics during debugging sessions. It uses hard-coded reasonable limits for output formatting and automatically includes summary information when the output would otherwise be very long. The function is designed primarily for debugging purposes and outputs all statistics to stderr for immediate visibility during development and troubleshooting.

## Parameters / Member Variables
- `context`: The memory context for which to print statistics, including all its descendant contexts in the context hierarchy

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextStatsDetail](MemoryContextStatsDetail.md)
- Called from (representative examples):
  - [finish_xact_command](../f/finish_xact_command.md) (transaction cleanup debugging)
  - [MemoryContextAllocationFailure](MemoryContextAllocationFailure.md) (error reporting)
  - [AllocSetContextCreateInternal](../A/AllocSetContextCreateInternal.md) (context creation debugging)
  - Various test functions for memory validation

## Notes and Other Information
- This is a debugging-only utility function and should not be used in production code paths
- Uses hard-wired limits (100, 100, true) for max_children, max_total_children, and include_details parameters
- Output is directed to stderr to avoid interfering with normal program output
- The function makes efforts to summarize output when dealing with large context hierarchies to keep the output manageable
- Commonly used during memory leak investigation and context hierarchy analysis

## Simplified Source

```c
// Simplified version of MemoryContextStats - Memory context statistics printer
void MemoryContextStats(MemoryContext context) {
    // Print stats with default limits: max 100 levels, max 100 children per level
    MemoryContextStatsDetail(context, 100, 100, true);
}

// Core statistics collection function
void MemoryContextStatsDetail(MemoryContext context, int max_level, int max_children, bool print_to_stderr) {
    MemoryContextCounters grand_totals;

    // Initialize totals counter
    memset(&grand_totals, 0, sizeof(grand_totals));

    // Recursively collect stats from context tree
    MemoryContextStatsInternal(context, 0, max_level, max_children, &grand_totals, print_to_stderr);

    // Print or log grand totals
    if (print_to_stderr) {
        fprintf(stderr, "Grand total: %zu bytes in %zu blocks; %zu free (%zu chunks); %zu used\n",
                grand_totals.totalspace, grand_totals.nblocks,
                grand_totals.freespace, grand_totals.freechunks,
                grand_totals.totalspace - grand_totals.freespace);
    } else {
        // Log using ereport for server-only logging
        ereport(LOG_SERVER_ONLY, (errmsg_internal("Grand total: %zu bytes in %zu blocks; %zu free (%zu chunks); %zu used",
                grand_totals.totalspace, grand_totals.nblocks, grand_totals.freespace,
                grand_totals.freechunks, grand_totals.totalspace - grand_totals.freespace)));
    }
}

// Recursive worker function
static void MemoryContextStatsInternal(MemoryContext context, int level, int max_level, int max_children,
                                     MemoryContextCounters *totals, bool print_to_stderr) {
    // Print stats for current context
    context->methods->stats(context, MemoryContextStatsPrint, &level, totals, print_to_stderr);

    // Process child contexts up to limits
    MemoryContext child = context->firstchild;
    int child_count = 0;

    // Recursively process children within depth and count limits
    if (level < max_level && !stack_is_too_deep()) {
        while (child != NULL && child_count < max_children) {
            MemoryContextStatsInternal(child, level + 1, max_level, max_children, totals, print_to_stderr);
            child = child->nextchild;
            child_count++;
        }
    }

    // Summarize remaining children without recursion
    if (child != NULL) {
        MemoryContextCounters remaining_totals;
        memset(&remaining_totals, 0, sizeof(remaining_totals));

        // Count remaining children and accumulate their stats
        int remaining_count = 0;
        while (child != NULL) {
            child->methods->stats(child, NULL, NULL, &remaining_totals, false);
            remaining_count++;
            child = MemoryContextTraverseNext(child, context);
        }

        // Print summary of remaining children
        if (print_to_stderr) {
            // Print indentation based on level
            for (int i = 0; i <= level; i++) fprintf(stderr, "  ");
            fprintf(stderr, "%d more child contexts containing %zu total in %zu blocks; %zu free (%zu chunks); %zu used\n",
                    remaining_count, remaining_totals.totalspace, remaining_totals.nblocks,
                    remaining_totals.freespace, remaining_totals.freechunks,
                    remaining_totals.totalspace - remaining_totals.freespace);
        } else {
            // Log summary using ereport
            ereport(LOG_SERVER_ONLY, (errmsg_internal("level: %d; %d more child contexts containing %zu total in %zu blocks",
                    level, remaining_count, remaining_totals.totalspace, remaining_totals.nblocks)));
        }

        // Add remaining totals to grand total
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
- Removed detailed error handling comments for brevity
- Consolidated variable declarations with initialization where possible
- Simplified complex conditional logic flows
- Added inline comments explaining the core algorithm steps
- Focused on the main execution path while preserving essential functionality
- Maintained the recursive tree traversal logic which is core to the function's purpose