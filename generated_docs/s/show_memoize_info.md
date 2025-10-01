# show_memoize_info

## Location
[src/backend/commands/explain.c:3327-3470](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L3327-L3470)

## Overview
Displays comprehensive memoization cache statistics for EXPLAIN ANALYZE output, including cache keys, hit/miss ratios, memory usage, and performance metrics from both leader and worker processes in parallel execution.

## Definition

```c
static void
show_memoize_info(MemoizeState *mstate, List *ancestors, ExplainState *es)
```
## Detailed Description
This function provides detailed reporting for memoization nodes during EXPLAIN ANALYZE operations. Memoization is a query optimization technique that caches results of expensive operations to avoid redundant computation. The function displays both configuration information and runtime performance statistics.

The function operates in several phases:
1. **Cache key display**: Deparses and displays the expressions used as cache keys
2. **Cache mode reporting**: Shows whether binary or logical comparison is used
3. **Performance statistics**: Displays hits, misses, evictions, overflows, and memory usage
4. **Parallel worker reporting**: Aggregates and displays statistics from all parallel workers

Key features include:
- Dynamic expression deparsing for cache key display with proper context
- Support for both binary and logical cache comparison modes
- Comprehensive cache performance metrics (hits, misses, evictions, overflows)
- Memory usage tracking with peak memory reporting
- Full parallel execution support with per-worker statistics
- Intelligent filtering to exclude inactive workers

The function handles the complexity of memory reporting where peak memory is only tracked when memory is freed, falling back to current usage when peak data is unavailable.

## Parameters / Member Variables
- : Pointer to MemoizeState structure containing memoization execution state and statistics
- : List of ancestor plan nodes used for expression deparsing context
- : Pointer to ExplainState structure containing output formatting context and control flags

## Dependencies
- Functions called/Symbols referenced:
  - [set_deparse_context_plan](set_deparse_context_plan.md): Sets up context for expression deparsing
  - [deparse_expression](../d/deparse_expression.md): Converts expression nodes back to SQL text
  - BYTES_TO_KILOBYTES: Converts memory usage from bytes to kilobytes
  - [ExplainPropertyText](../E/ExplainPropertyText.md)/ExplainPropertyInteger: Adds properties to structured output
  - [ExplainIndentText](../E/ExplainIndentText.md): Handles text output indentation
  - [appendStringInfo](../a/appendStringInfo.md): Formats and appends text to output buffer
  - [ExplainOpenWorker](../E/ExplainOpenWorker.md)/ExplainCloseWorker: Manages worker-specific output sections
  - [initStringInfo](../i/initStringInfo.md)/pfree: String buffer management
- Called from (representative examples):
  - [ExplainNode](../E/ExplainNode.md): Main EXPLAIN node processing function for memoize nodes

## Notes and Other Information
- This is a static function used internally within explain.c for memoization reporting
- Cache keys are displayed as comma-separated deparse expressions for readability
- The function distinguishes between binary mode (faster, binary comparison) and logical mode (slower, value-based comparison)
- Memory usage reporting handles the edge case where mem_peak is zero by falling back to current memory usage
- Worker statistics are only displayed for workers that performed actual work (cache_misses > 0)
- The function respects the useprefix logic for expression display based on table count and verbosity
- Early return when es->analyze is false ensures no overhead during regular EXPLAIN (without ANALYZE)
- Memory usage is always reported in kilobytes for consistency with other PostgreSQL memory reporting
- Cache overflows indicate when the cache size limit was exceeded and new entries couldn't be stored
- Cache evictions show when older entries were removed to make room for new ones
- The parallel worker section properly handles the case where worker MemoizeState.mem_used is not accessible

## Simplified Source

```c
static void
show_memoize_info(MemoizeState *mstate, List *ancestors, ExplainState *es)
{
    Plan *plan = ((PlanState *) mstate)->plan;
    StringInfoData keystr;
    bool useprefix;
    int64 memPeakKb;

    initStringInfo(&keystr);

    // Determine whether to use table prefixes in expression display
    useprefix = list_length(es->rtable) > 1 || es->verbose;

    // Set up context for expression deparsing
    List *context = set_deparse_context_plan(es->deparse_cxt, plan, ancestors);

    // Build cache key string from parameter expressions
    char *separator = "";
    foreach(lc, ((Memoize *) plan)->param_exprs)
    {
        Node *expr = (Node *) lfirst(lc);
        appendStringInfoString(&keystr, separator);
        appendStringInfoString(&keystr, deparse_expression(expr, context, useprefix, false));
        separator = ", ";
    }

    // Display cache key and mode information
    if (es->format != EXPLAIN_FORMAT_TEXT)
    {
        ExplainPropertyText("Cache Key", keystr.data, es);
        ExplainPropertyText("Cache Mode", mstate->binary_mode ? "binary" : "logical", es);
    }
    else
    {
        ExplainIndentText(es);
        appendStringInfo(es->str, "Cache Key: %s\n", keystr.data);
        ExplainIndentText(es);
        appendStringInfo(es->str, "Cache Mode: %s\n", mstate->binary_mode ? "binary" : "logical");
    }

    pfree(keystr.data);

    // Only show statistics if ANALYZE was used
    if (!es->analyze)
        return;

    // Display cache statistics if there were cache operations
    if (mstate->stats.cache_misses > 0)
    {
        // Calculate peak memory usage (use current if peak not tracked)
        memPeakKb = (mstate->stats.mem_peak > 0) ?
                   BYTES_TO_KILOBYTES(mstate->stats.mem_peak) :
                   BYTES_TO_KILOBYTES(mstate->mem_used);

        // Display statistics in appropriate format
        if (es->format != EXPLAIN_FORMAT_TEXT)
        {
            ExplainPropertyInteger("Cache Hits", NULL, mstate->stats.cache_hits, es);
            ExplainPropertyInteger("Cache Misses", NULL, mstate->stats.cache_misses, es);
            ExplainPropertyInteger("Cache Evictions", NULL, mstate->stats.cache_evictions, es);
            ExplainPropertyInteger("Cache Overflows", NULL, mstate->stats.cache_overflows, es);
            ExplainPropertyInteger("Peak Memory Usage", "kB", memPeakKb, es);
        }
        else
        {
            ExplainIndentText(es);
            appendStringInfo(es->str,
                "Hits: %lu  Misses: %lu  Evictions: %lu  Overflows: %lu  Memory Usage: %ldkB\n",
                mstate->stats.cache_hits, mstate->stats.cache_misses,
                mstate->stats.cache_evictions, mstate->stats.cache_overflows, memPeakKb);
        }
    }

    // Display statistics from parallel workers if available
    if (mstate->shared_info != NULL)
    {
        for (int n = 0; n < mstate->shared_info->num_workers; n++)
        {
            MemoizeInstrumentation *si = &mstate->shared_info->sinstrument[n];

            // Skip workers that didn't perform any work
            if (si->cache_misses == 0)
                continue;

            if (es->workers_state)
                ExplainOpenWorker(n, es);

            // Worker memory peak is already set by ExecEndMemoize
            memPeakKb = BYTES_TO_KILOBYTES(si->mem_peak);

            // Display worker statistics
            if (es->format == EXPLAIN_FORMAT_TEXT)
            {
                ExplainIndentText(es);
                appendStringInfo(es->str,
                    "Hits: %lu  Misses: %lu  Evictions: %lu  Overflows: %lu  Memory Usage: %ldkB\n",
                    si->cache_hits, si->cache_misses, si->cache_evictions,
                    si->cache_overflows, memPeakKb);
            }
            else
            {
                ExplainPropertyInteger("Cache Hits", NULL, si->cache_hits, es);
                ExplainPropertyInteger("Cache Misses", NULL, si->cache_misses, es);
                ExplainPropertyInteger("Cache Evictions", NULL, si->cache_evictions, es);
                ExplainPropertyInteger("Cache Overflows", NULL, si->cache_overflows, es);
                ExplainPropertyInteger("Peak Memory Usage", "kB", memPeakKb, es);
            }

            if (es->workers_state)
                ExplainCloseWorker(n, es);
        }
    }
}
```