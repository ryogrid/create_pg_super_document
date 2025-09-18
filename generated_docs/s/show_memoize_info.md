# show_memoize_info

## Location
src/backend/commands/explain.c: 3327 - 3470

## Overview
Displays comprehensive memoization cache statistics for EXPLAIN ANALYZE output, including cache keys, hit/miss ratios, memory usage, and performance metrics from both leader and worker processes in parallel execution.

## Definition


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
  - appendStringInfo: Formats and appends text to output buffer
  - [ExplainOpenWorker](../E/ExplainOpenWorker.md)/ExplainCloseWorker: Manages worker-specific output sections
  - initStringInfo/pfree: String buffer management
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