# ExplainMissingMembers

## Location
src/backend/commands/explain.c: 4402 - 4415

## Overview
ExplainMissingMembers is a static function that reports information about pruned subnodes in Append or MergeAppend nodes during PostgreSQL's EXPLAIN output.

## Definition
```c
static void ExplainMissingMembers(int nplans, int nchildren, ExplainState *es)
```

## Detailed Description
This function displays information about runtime partition pruning in Append and MergeAppend nodes. When PostgreSQL's partition pruning optimization removes some child plans at execution time (because they're determined to be unnecessary based on runtime parameters), this function reports how many subplans were removed. The function only shows this information when subplans were actually pruned (nplans < nchildren) or when using non-text output formats where the information is always included for completeness.

## Parameters / Member Variables
- `nplans`: Number of live/active subplans that will be executed
- `nchildren`: Original number of subnodes in the Plan before any pruning occurred
- `es`: Pointer to ExplainState structure controlling output format and options

## Dependencies
- Functions called/Symbols referenced:
  - [ExplainPropertyInteger](ExplainPropertyInteger.md)
  - ExplainState (struct)
  - EXPLAIN_FORMAT_TEXT (constant)
- Called from (representative examples):
  - [ExplainNode](ExplainNode.md) (for Append and MergeAppend nodes)

## Notes and Other Information
- This is a static function, only accessible within the explain.c file
- Specifically handles runtime partition pruning reporting for composite nodes
- Shows "Subplans Removed" property indicating how many child plans were pruned
- The condition (nplans < nchildren || es->format \!= EXPLAIN_FORMAT_TEXT) ensures the information is shown when relevant or when using structured output formats
- Part of PostgreSQL's partition pruning optimization reporting system
- Helps users understand the effectiveness of runtime partition elimination
- Related to PostgreSQL's partitioned table performance optimizations