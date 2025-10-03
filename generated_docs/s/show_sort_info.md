# show_sort_info

## Location
[src/backend/commands/explain.c:2945-3035](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L2945-L3035)

## Overview
A static function that displays detailed sorting statistics and performance information for Sort nodes in PostgreSQL's EXPLAIN ANALYZE output, including sort method, space usage, and parallel worker information.

## Definition
```c
static void
show_sort_info(SortState *sortstate, ExplainState *es)
```

## Detailed Description
The `show_sort_info` function provides detailed performance statistics for sorting operations when EXPLAIN ANALYZE is used. It extracts statistics from the tuplesort subsystem including the sort method used (quicksort, heapsort, external sort), space usage, and space type (memory vs disk). The function handles both single-process and parallel sorting scenarios, displaying statistics for each worker process when parallel sorting is employed.

The function only operates during EXPLAIN ANALYZE (when es->analyze is true) and only after the sort operation has completed. It formats the output differently for text vs structured formats, and includes special handling for parallel workers, including the ability to hide worker details when requested while still showing aggregate information.

## Parameters / Member Variables
- `sortstate`: Pointer to the SortState execution state containing sorting statistics and instrumentation
- `es`: ExplainState containing output formatting options and analysis flags

## Dependencies
- Functions called/Symbols referenced:
  - [tuplesort_get_stats](../t/tuplesort_get_stats.md) (retrieves statistics from the tuplesort subsystem)
  - [tuplesort_method_name](../t/tuplesort_method_name.md) (converts sort method enum to human-readable name)
  - [tuplesort_space_type_name](../t/tuplesort_space_type_name.md) (converts space type enum to human-readable name)
  - [ExplainIndentText](../E/ExplainIndentText.md) (adds proper indentation in text format)
  - [ExplainPropertyText](../E/ExplainPropertyText.md) (outputs named text properties in structured formats)
  - [ExplainPropertyInteger](../E/ExplainPropertyInteger.md) (outputs named integer properties in structured formats)
  - [ExplainOpenWorker](../E/ExplainOpenWorker.md)/ExplainCloseWorker (handles parallel worker output formatting)
  - [appendStringInfo](../a/appendStringInfo.md) (builds output string)
- Types referenced:
  - [SortState](../S/SortState.md), ExplainState, Tuplesortstate, TuplesortInstrumentation
- Constants referenced:
  - EXPLAIN_FORMAT_TEXT, SORT_TYPE_STILL_IN_PROGRESS, INT64_FORMAT
- Called from (representative examples):
  - [ExplainNode](../E/ExplainNode.md) (when explaining Sort plan nodes during EXPLAIN ANALYZE)

## Notes and Other Information
- Only executes when es->analyze is true (EXPLAIN ANALYZE mode)
- Only displays information if the sort operation has completed (sort_Done is true)
- Displays sort method (e.g., "quicksort", "external merge"), space type ("Memory" or "Disk"), and space used in kilobytes
- Handles parallel execution by iterating through worker statistics in shared_info
- Includes logic to handle the es->hide_workers flag by showing worker 0's data as top-level data
- Skips incomplete worker slots (SORT_TYPE_STILL_IN_PROGRESS)
- Output format differs between text ("Sort Method: quicksort Memory: 1234kB") and structured formats (separate properties)
- Provides crucial performance debugging information for understanding sort operation efficiency
- Space usage statistics help identify whether sorts are memory-bound or spilling to disk

## Simplified Source

```c
static void show_sort_info(SortState *sortstate, ExplainState *es) {
    // Only show info for EXPLAIN ANALYZE
    if (!es->analyze)
        return;

    // Show main process sort statistics if sort completed
    if (sortstate->sort_Done && sortstate->tuplesortstate != NULL) {
        TuplesortInstrumentation stats;
        tuplesort_get_stats(sortstate->tuplesortstate, &stats);

        const char *sortMethod = tuplesort_method_name(stats.sortMethod);
        const char *spaceType = tuplesort_space_type_name(stats.spaceType);
        int64 spaceUsed = stats.spaceUsed;

        // Output format depends on text vs structured output
        if (es->format == EXPLAIN_FORMAT_TEXT) {
            ExplainIndentText(es);
            appendStringInfo(es->str, "Sort Method: %s  %s: %ldkB\n",
                           sortMethod, spaceType, spaceUsed);
        } else {
            ExplainPropertyText("Sort Method", sortMethod, es);
            ExplainPropertyInteger("Sort Space Used", "kB", spaceUsed, es);
            ExplainPropertyText("Sort Space Type", spaceType, es);
        }
    }

    // Show parallel worker statistics if available
    if (sortstate->shared_info != NULL) {
        for (int n = 0; n < sortstate->shared_info->num_workers; n++) {
            TuplesortInstrumentation *worker_stats = &sortstate->shared_info->sinstrument[n];

            // Skip incomplete workers
            if (worker_stats->sortMethod == SORT_TYPE_STILL_IN_PROGRESS)
                continue;

            const char *sortMethod = tuplesort_method_name(worker_stats->sortMethod);
            const char *spaceType = tuplesort_space_type_name(worker_stats->spaceType);
            int64 spaceUsed = worker_stats->spaceUsed;

            // Handle worker output formatting
            if (es->workers_state)
                ExplainOpenWorker(n, es);

            // Output worker statistics in appropriate format
            if (es->format == EXPLAIN_FORMAT_TEXT) {
                ExplainIndentText(es);
                appendStringInfo(es->str, "Sort Method: %s  %s: %ldkB\n",
                               sortMethod, spaceType, spaceUsed);
            } else {
                ExplainPropertyText("Sort Method", sortMethod, es);
                ExplainPropertyInteger("Sort Space Used", "kB", spaceUsed, es);
                ExplainPropertyText("Sort Space Type", spaceType, es);
            }

            if (es->workers_state)
                ExplainCloseWorker(n, es);
        }
    }
}
```