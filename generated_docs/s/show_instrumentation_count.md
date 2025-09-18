# show_instrumentation_count

## Location
src/backend/commands/explain.c: 3622 - 3650

## Overview
Displays instrumentation counter statistics for plan nodes during EXPLAIN ANALYZE, specifically showing the average number of rows filtered per execution loop.

## Definition
```c
static void show_instrumentation_count(const char *qlabel, int which, PlanState *planstate, ExplainState *es)
```

## Detailed Description
This function extracts and displays filtering statistics from plan node instrumentation data during EXPLAIN ANALYZE. It calculates the average number of rows filtered per loop execution and formats this information according to the specified explain format. The function supports two different filtering counters (nfiltered1 and nfiltered2) depending on the 'which' parameter, allowing different types of filtering operations to be tracked separately.

The function automatically suppresses zero counts in text mode to avoid cluttering the output with uninteresting information, but includes them in structured formats like JSON for completeness.

## Parameters / Member Variables
- `qlabel`: Label string to display for this counter in the explain output
- `which`: Identifies which instrumentation counter to use (2 for nfiltered2, any other value for nfiltered1)  
- `planstate`: PlanState containing the instrumentation data with filtering counters and loop counts
- `es`: ExplainState containing output formatting information and analysis flags

## Dependencies
- Functions called/Symbols referenced:
  - [ExplainPropertyFloat](../E/ExplainPropertyFloat.md)
  - EXPLAIN_FORMAT_TEXT
- Called from (representative examples):
  - [ExplainNode](../E/ExplainNode.md) (multiple locations for different plan node types)
  - [show_modifytable_info](show_modifytable_info.md)

## Notes and Other Information
- Only operates during EXPLAIN ANALYZE when instrumentation is enabled
- Returns early if analysis is disabled or no instrumentation data is available
- Handles the case where nloops is zero by displaying 0.0 instead of attempting division by zero
- Widely used throughout ExplainNode function to display filtering statistics for various plan node types
- The two filtering counters allow tracking different types of filtering operations within the same node