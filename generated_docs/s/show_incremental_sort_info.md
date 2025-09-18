# show_incremental_sort_info

## Location
src/backend/commands/explain.c: 3150 - 3235

## Overview
Displays comprehensive tuplesort statistics for incremental sort nodes in EXPLAIN ANALYZE output, handling both leader and worker process statistics in parallel execution scenarios.

## Definition


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
- : Pointer to IncrementalSortState structure containing execution state and statistics for the incremental sort node
- : Pointer to ExplainState structure containing output formatting context and control flags

## Dependencies
- Functions called/Symbols referenced:
  - [show_incremental_sort_group_info](show_incremental_sort_group_info.md): Displays statistics for individual sort groups
  - appendStringInfoChar: Adds newline characters for text formatting
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