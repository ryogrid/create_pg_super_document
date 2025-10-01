# show_hash_info

## Location
[src/backend/commands/explain.c:3236-3326](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L3236-L3326)

## Overview
Displays hash join statistics for EXPLAIN ANALYZE output, including bucket counts, batch information, and memory usage, with support for aggregating data from parallel worker processes.

## Definition

```c
static void
show_hash_info(HashState *hashstate, ExplainState *es)
```
## Detailed Description
This function is responsible for collecting and displaying comprehensive hash join execution statistics during EXPLAIN ANALYZE operations. It handles the complexity of parallel hash joins by aggregating instrumentation data from all participating processes (leader and workers).

The function performs several key operations:
1. **Local data collection**: Copies instrumentation data from the current process
2. **Parallel data aggregation**: Merges statistics from all worker processes using maximum values
3. **Intelligent reporting**: Displays different levels of detail based on whether hash table parameters changed during execution

Key features include:
- Handles both parallel-oblivious and parallel-aware hash join scenarios
- Reports original vs. final bucket and batch counts when they differ due to dynamic adjustments
- Aggregates peak memory usage across all participants
- Supports both structured (JSON/XML/YAML) and text output formats
- Uses maximum values for aggregation to capture worst-case resource usage

The aggregation strategy takes maximum values across workers because each worker may process different subsets of data and we want to report the highest resource usage encountered by any participant.

## Parameters / Member Variables
- : Pointer to HashState structure containing hash join execution state and instrumentation data
- : Pointer to ExplainState structure containing output formatting context and buffers

## Dependencies
- Functions called/Symbols referenced:
  - memcpy: Copies instrumentation data from local process
  - BYTES_TO_KILOBYTES: Converts memory usage from bytes to kilobytes
  - [ExplainPropertyInteger](../E/ExplainPropertyInteger.md)/ExplainPropertyUInteger: Adds properties to structured output
  - [ExplainIndentText](../E/ExplainIndentText.md): Handles text output indentation
  - [appendStringInfo](../a/appendStringInfo.md): Formats and appends text to output buffer
  - Max: Macro for finding maximum values during aggregation
- Called from (representative examples):
  - [ExplainNode](../E/ExplainNode.md): Main EXPLAIN node processing function for hash join nodes

## Notes and Other Information
- This is a static function used internally within explain.c for hash join reporting
- The function handles edge cases where some participants may not have built hash tables due to timing or empty input
- Memory usage is always reported in kilobytes for consistency with other PostgreSQL memory reporting
- Original vs. final parameter reporting helps users understand dynamic hash table adjustments
- The function only displays information when nbatch > 0, indicating actual hash table construction occurred
- Text format provides two different output styles: detailed (when parameters changed) and simplified (when parameters remained constant)
- Parallel aggregation uses maximum values rather than sums because we want to show peak resource usage per process
- The function properly handles cases where workers may have different instrumentation data due to work distribution differences

## Simplified Source

```c
static void
show_hash_info(HashState *hashstate, ExplainState *es)
{
    HashInstrumentation hinstrument = {0};

    // Collect stats from local process
    if (hashstate->hinstrument)
        memcpy(&hinstrument, hashstate->hinstrument, sizeof(HashInstrumentation));

    // Merge results from parallel workers (take maximum values)
    if (hashstate->shared_info)
    {
        SharedHashInfo *shared_info = hashstate->shared_info;

        for (int i = 0; i < shared_info->num_workers; ++i)
        {
            HashInstrumentation *worker_hi = &shared_info->hinstrument[i];

            hinstrument.nbuckets = Max(hinstrument.nbuckets, worker_hi->nbuckets);
            hinstrument.nbuckets_original = Max(hinstrument.nbuckets_original, worker_hi->nbuckets_original);
            hinstrument.nbatch = Max(hinstrument.nbatch, worker_hi->nbatch);
            hinstrument.nbatch_original = Max(hinstrument.nbatch_original, worker_hi->nbatch_original);
            hinstrument.space_peak = Max(hinstrument.space_peak, worker_hi->space_peak);
        }
    }

    // Display hash statistics if any batches were used
    if (hinstrument.nbatch > 0)
    {
        uint64 spacePeakKb = BYTES_TO_KILOBYTES(hinstrument.space_peak);

        if (es->format != EXPLAIN_FORMAT_TEXT)
        {
            // Structured output format
            ExplainPropertyInteger("Hash Buckets", NULL, hinstrument.nbuckets, es);
            ExplainPropertyInteger("Original Hash Buckets", NULL, hinstrument.nbuckets_original, es);
            ExplainPropertyInteger("Hash Batches", NULL, hinstrument.nbatch, es);
            ExplainPropertyInteger("Original Hash Batches", NULL, hinstrument.nbatch_original, es);
            ExplainPropertyUInteger("Peak Memory Usage", "kB", spacePeakKb, es);
        }
        else
        {
            // Text output - show detailed or simple format based on whether values changed
            ExplainIndentText(es);
            if (hinstrument.nbatch_original != hinstrument.nbatch ||
                hinstrument.nbuckets_original != hinstrument.nbuckets)
            {
                // Show original vs final values when they differ
                appendStringInfo(es->str,
                    "Buckets: %d (originally %d)  Batches: %d (originally %d)  Memory Usage: %lukB\n",
                    hinstrument.nbuckets, hinstrument.nbuckets_original,
                    hinstrument.nbatch, hinstrument.nbatch_original, spacePeakKb);
            }
            else
            {
                // Simple format when values didn't change
                appendStringInfo(es->str,
                    "Buckets: %d  Batches: %d  Memory Usage: %lukB\n",
                    hinstrument.nbuckets, hinstrument.nbatch, spacePeakKb);
            }
        }
    }
}
```