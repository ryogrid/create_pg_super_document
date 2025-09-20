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
  - appendStringInfo: Formats and appends text to output buffer
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