# show_incremental_sort_info

## Location
[src/backend/commands/explain.c:3150-3235](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L3150-L3235)

## Overview
Displays comprehensive tuplesort statistics for incremental sort nodes in EXPLAIN ANALYZE output, handling both leader and worker process statistics in parallel execution scenarios.

## Definition

```c
static void
show_incremental_sort_info(IncrementalSortState *incrsortstate,
						   ExplainState *es)
```
## Detailed Description
This function is the main entry point for displaying incremental sort statistics during EXPLAIN ANALYZE operations. It orchestrates the display of sort statistics for both the leader process and any parallel worker processes that participated in the incremental sort operation.

The function handles two distinct types of sort groups that can occur in incremental sorting:
1. **Full-sort groups**: Groups where a complete sort was required
2. **Pre-sorted groups**: Groups that were already partially sorted and required less work

Key behavioral aspects include:
- Only displays statistics when EXPLAIN ANALYZE is active (es->analyze is true)
- Handles both single-process and parallel execution scenarios
- Excludes workers that didn't contribute meaningfully (zero groups processed)
- Provides proper formatting and indentation for text output
- Manages the display of worker-specific statistics using ExplainOpenWorker/ExplainCloseWorker

The function implements intelligent filtering logic - it only shows prefix sort groups if full sort groups exist, reflecting the reality that incremental sort transitions from full sorting to prefix sorting as data becomes more ordered.

## Parameters / Member Variables
- `*incrsortstate`: Pointer to IncrementalSortState structure containing execution state and statistics for the incremental sort node
- `*es`: Pointer to ExplainState structure containing output formatting context and control flags
## Dependencies
- Functions called/Symbols referenced:
  - [show_incremental_sort_group_info](show_incremental_sort_group_info.md): Displays statistics for individual sort groups
  - [appendStringInfoChar](../a/appendStringInfoChar.md): Adds newline characters for text formatting
  - [ExplainOpenWorker](../E/ExplainOpenWorker.md)/ExplainCloseWorker: Manages worker-specific output sections
- Called from (representative examples):
  - [ExplainNode](../E/ExplainNode.md): Main EXPLAIN node processing function

## Notes and Other Information
- This is a static function used internally within explain.c for incremental sort reporting
- The function implements sophisticated logic to avoid displaying empty or meaningless statistics
- Worker processes are only included in output if they processed at least one sort group
- The function handles the transition between full-sort and pre-sorted groups that occurs as incremental sort optimizes its behavior
- Text format output includes careful newline management for proper spacing between different sections
- Parallel execution statistics are properly nested within worker-specific sections
- The function respects the verbose flag when determining indentation for worker output
- Early return when es->analyze is false ensures no overhead during regular EXPLAIN (without ANALYZE)

## Simplified Source

```c
static void
show_incremental_sort_info(IncrementalSortState *incrsortstate, ExplainState *es)
{
    IncrementalSortGroupInfo *fullsortGroupInfo;
    IncrementalSortGroupInfo *prefixsortGroupInfo;

    if (!es->analyze)
        return;

    fullsortGroupInfo = &incrsortstate->incsort_info.fullsortGroupInfo;

    // Show stats for leader process if it did any full sorting
    if (fullsortGroupInfo->groupCount > 0)
    {
        show_incremental_sort_group_info(fullsortGroupInfo, "Full-sort", true, es);

        prefixsortGroupInfo = &incrsortstate->incsort_info.prefixsortGroupInfo;
        if (prefixsortGroupInfo->groupCount > 0)
        {
            if (es->format == EXPLAIN_FORMAT_TEXT)
                appendStringInfoChar(es->str, '\n');
            show_incremental_sort_group_info(prefixsortGroupInfo, "Pre-sorted", true, es);
        }
        if (es->format == EXPLAIN_FORMAT_TEXT)
            appendStringInfoChar(es->str, '\n');
    }

    // Show stats for parallel workers that contributed
    if (incrsortstate->shared_info != NULL)
    {
        for (int n = 0; n < incrsortstate->shared_info->num_workers; n++)
        {
            IncrementalSortInfo *incsort_info = &incrsortstate->shared_info->sinfo[n];
            fullsortGroupInfo = &incsort_info->fullsortGroupInfo;

            // Skip workers that didn't process any groups
            if (fullsortGroupInfo->groupCount == 0)
                continue;

            if (es->workers_state)
                ExplainOpenWorker(n, es);

            bool indent_first_line = es->workers_state == NULL || es->verbose;
            show_incremental_sort_group_info(fullsortGroupInfo, "Full-sort", indent_first_line, es);

            prefixsortGroupInfo = &incsort_info->prefixsortGroupInfo;
            if (prefixsortGroupInfo->groupCount > 0)
            {
                if (es->format == EXPLAIN_FORMAT_TEXT)
                    appendStringInfoChar(es->str, '\n');
                show_incremental_sort_group_info(prefixsortGroupInfo, "Pre-sorted", true, es);
            }
            if (es->format == EXPLAIN_FORMAT_TEXT)
                appendStringInfoChar(es->str, '\n');

            if (es->workers_state)
                ExplainCloseWorker(n, es);
        }
    }
}
```