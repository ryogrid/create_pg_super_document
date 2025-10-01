# show_hashagg_info

## Location
[src/backend/commands/explain.c:3471-3591](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L3471-L3591)

## Overview
Displays comprehensive hash aggregation statistics for EXPLAIN ANALYZE output, including planned partitions, batch counts, memory usage, disk spill information, and performance metrics from parallel workers.

## Definition

```c
static void
show_hashagg_info(AggState *aggstate, ExplainState *es)
```
## Detailed Description
This function provides detailed reporting for hash aggregation nodes during EXPLAIN ANALYZE operations. Hash aggregation is a query execution technique that groups rows using hash tables, which may need to spill to disk when memory is insufficient. The function displays both planning information and runtime performance statistics.

The function operates in several phases:
1. **Strategy validation**: Only processes AGG_HASHED and AGG_MIXED aggregation strategies
2. **Planning information**: Shows planned partitions when cost information is requested
3. **Runtime statistics**: Displays actual batches used, peak memory usage, and disk usage
4. **Parallel worker reporting**: Shows statistics from all parallel workers that performed work

Key features include:
- Support for both hashed and mixed aggregation strategies
- Planned vs. actual partition reporting for cost analysis
- Memory and disk usage tracking with intelligent formatting
- Conditional disk usage display (only when spilling occurred)
- Parallel execution support with per-worker statistics
- Intelligent filtering to exclude inactive participants (leader or workers)

The function handles the complexity of parallel aggregation where the leader process may not participate in the actual work, detecting this condition by examining memory usage patterns.

## Parameters / Member Variables
- : Pointer to AggState structure containing aggregation execution state and performance statistics
- : Pointer to ExplainState structure containing output formatting context and control flags

## Dependencies
- Functions called/Symbols referenced:
  - BYTES_TO_KILOBYTES: Converts memory usage from bytes to kilobytes
  - [ExplainPropertyInteger](../E/ExplainPropertyInteger.md): Adds integer properties to structured output
  - [ExplainIndentText](../E/ExplainIndentText.md): Handles text output indentation
  - [appendStringInfo](../a/appendStringInfo.md)/appendStringInfoSpaces: Formats and appends text to output buffer
  - [ExplainOpenWorker](../E/ExplainOpenWorker.md)/ExplainCloseWorker: Manages worker-specific output sections
  - [appendStringInfoChar](../a/appendStringInfoChar.md): Adds single characters to output buffer
- Called from (representative examples):
  - [ExplainNode](../E/ExplainNode.md): Main EXPLAIN node processing function for aggregation nodes

## Notes and Other Information
- This is a static function used internally within explain.c for hash aggregation reporting
- The function early-returns for non-hash aggregation strategies (AGG_PLAIN, AGG_SORTED)
- Planned partitions are only shown when cost information is requested (es->costs is true)
- Disk usage is only displayed when batching occurred (hash_batches_used > 1), indicating memory pressure
- The function detects inactive participants by checking hash_mem_peak > 0
- Memory usage is always reported in kilobytes for consistency with other PostgreSQL memory reporting
- Text format provides compact single-line output with conditional disk usage information
- Worker statistics are only displayed for workers that performed actual aggregation work
- The function handles both structured (JSON/XML/YAML) and text output formats appropriately
- Batch count greater than 1 indicates that the hash table exceeded memory limits and required disk spilling
- The gotone flag in text formatting ensures proper spacing and newlines between different information sections
- During parallel execution, each worker maintains separate instrumentation data that is displayed individually

## Simplified Source

```c
static void
show_hashagg_info(AggState *aggstate, ExplainState *es)
{
    Agg *agg = (Agg *) aggstate->ss.ps.plan;
    int64 memPeakKb = BYTES_TO_KILOBYTES(aggstate->hash_mem_peak);

    // Only process hash and mixed aggregation strategies
    if (agg->aggstrategy != AGG_HASHED && agg->aggstrategy != AGG_MIXED)
        return;

    if (es->format != EXPLAIN_FORMAT_TEXT)
    {
        // Structured output format
        if (es->costs)
            ExplainPropertyInteger("Planned Partitions", NULL, aggstate->hash_planned_partitions, es);

        // Only show runtime stats if this process did work (hash_mem_peak > 0)
        if (es->analyze && aggstate->hash_mem_peak > 0)
        {
            ExplainPropertyInteger("HashAgg Batches", NULL, aggstate->hash_batches_used, es);
            ExplainPropertyInteger("Peak Memory Usage", "kB", memPeakKb, es);
            ExplainPropertyInteger("Disk Usage", "kB", aggstate->hash_disk_used, es);
        }
    }
    else
    {
        // Text output format
        bool gotone = false;

        // Show planned partitions if requested
        if (es->costs && aggstate->hash_planned_partitions > 0)
        {
            ExplainIndentText(es);
            appendStringInfo(es->str, "Planned Partitions: %d", aggstate->hash_planned_partitions);
            gotone = true;
        }

        // Show runtime statistics if this process did work
        if (es->analyze && aggstate->hash_mem_peak > 0)
        {
            if (!gotone)
                ExplainIndentText(es);
            else
                appendStringInfoSpaces(es->str, 2);

            appendStringInfo(es->str, "Batches: %d  Memory Usage: %ldkB",
                            aggstate->hash_batches_used, memPeakKb);

            // Only show disk usage if spilling occurred
            if (aggstate->hash_batches_used > 1)
                appendStringInfo(es->str, "  Disk Usage: %lukB", aggstate->hash_disk_used);

            gotone = true;
        }

        if (gotone)
            appendStringInfoChar(es->str, '\n');
    }

    // Display stats for each parallel worker that did work
    if (es->analyze && aggstate->shared_info != NULL)
    {
        for (int n = 0; n < aggstate->shared_info->num_workers; n++)
        {
            AggregateInstrumentation *sinstrument = &aggstate->shared_info->sinstrument[n];

            // Skip workers that didn't do any work
            if (sinstrument->hash_mem_peak == 0)
                continue;

            memPeakKb = BYTES_TO_KILOBYTES(sinstrument->hash_mem_peak);

            if (es->workers_state)
                ExplainOpenWorker(n, es);

            if (es->format == EXPLAIN_FORMAT_TEXT)
            {
                ExplainIndentText(es);
                appendStringInfo(es->str, "Batches: %d  Memory Usage: %ldkB",
                                sinstrument->hash_batches_used, memPeakKb);

                if (sinstrument->hash_batches_used > 1)
                    appendStringInfo(es->str, "  Disk Usage: %lukB", sinstrument->hash_disk_used);
                appendStringInfoChar(es->str, '\n');
            }
            else
            {
                ExplainPropertyInteger("HashAgg Batches", NULL, sinstrument->hash_batches_used, es);
                ExplainPropertyInteger("Peak Memory Usage", "kB", memPeakKb, es);
                ExplainPropertyInteger("Disk Usage", "kB", sinstrument->hash_disk_used, es);
            }

            if (es->workers_state)
                ExplainCloseWorker(n, es);
        }
    }
}
```