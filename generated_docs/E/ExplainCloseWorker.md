# ExplainCloseWorker

## Location
[src/backend/commands/explain.c:4560-4595](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L4560-L4595)

## Overview
ExplainCloseWorker is a static function in PostgreSQL's explain module that ends output redirection for a specific worker's buffer and restores the previous output context during parallel query execution explanation.

## Definition

```c
static void
ExplainCloseWorker(int n, ExplainState *es)
```
## Detailed Description
This function serves as the counterpart to ExplainOpenWorker, properly terminating the worker-specific output session. It performs several critical tasks: saves the current formatting state for potential future use, handles format-specific cleanup (particularly for TEXT format), and restores the previous output buffer pointer.

A key feature is its intelligent handling of TEXT format output: if no actual content was produced for the worker (only the "Worker N:" prefix), it truncates the partial line to avoid displaying empty worker entries. This prevents misleading output when certain statistics (like buffer usage) are not applicable or available for specific workers.

The function also manages indentation levels and ensures that the formatting stack is properly maintained for potential subsequent ExplainOpenWorker calls on the same worker.

## Parameters / Member Variables
- `n`: The worker number/index (0-based) to close output for
- `*es`: ExplainState structure containing formatting options and worker state
## Dependencies
- Functions called/Symbols referenced:
  - Assert (validates worker state and that worker was previously opened)
  - [ExplainSaveGroup](ExplainSaveGroup.md) (saves current formatting state for future restoration)
  - EXPLAIN_FORMAT_TEXT (format comparison constant)
- Called from:
  - [ExplainNode](ExplainNode.md) (various locations when finishing parallel execution data collection)
  - [show_sort_info](../s/show_sort_info.md), show_incremental_sort_info, show_memoize_info, show_hashagg_info (after worker-specific statistics)

## Notes and Other Information
- Must be paired with a previous ExplainOpenWorker call for the same worker
- Saves formatting state to enable seamless resumption of worker output later
- Intelligently truncates empty worker lines in TEXT format to avoid clutter
- Restores the indent level that was incremented by ExplainOpenWorker
- Essential for maintaining clean, organized output when workers have no data to report
- Part of the worker output management system that enables coherent parallel execution statistics
- File location: src/backend/commands/explain.c:4560-4595

## Simplified Source

```c
static void
ExplainCloseWorker(int n, ExplainState *es)
{
    ExplainWorkersState *wstate = es->workers_state;

    // Validate worker state
    Assert(wstate && n >= 0 && n < wstate->num_workers && wstate->worker_inited[n]);

    // Save current formatting state for potential future use
    ExplainSaveGroup(es, 2, &wstate->worker_state_save[n]);

    // Handle TEXT format cleanup - remove empty worker lines
    if (es->format == EXPLAIN_FORMAT_TEXT) {
        // Truncate partial line if no content was added
        while (es->str->len > 0 && es->str->data[es->str->len - 1] != '\n')
            es->str->data[--(es->str->len)] = '\0';

        // Restore indentation level
        es->indent--;
    }

    // Restore previous output buffer
    es->str = wstate->prev_str;
}
```