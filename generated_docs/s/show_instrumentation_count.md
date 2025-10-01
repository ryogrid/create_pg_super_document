# show_instrumentation_count

## Location
[src/backend/commands/explain.c:3622-3650](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L3622-L3650)

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

## Simplified Source

```c
static void
show_instrumentation_count(const char *qlabel, int which,
                           PlanState *planstate, ExplainState *es)
{
    double nfiltered;
    double nloops;

    if (!es->analyze || !planstate->instrument)
        return;

    // Select which filtering counter to use
    if (which == 2)
        nfiltered = planstate->instrument->nfiltered2;
    else
        nfiltered = planstate->instrument->nfiltered1;
    nloops = planstate->instrument->nloops;

    // Suppress zero counts in text mode, but show them in structured formats
    if (nfiltered > 0 || es->format != EXPLAIN_FORMAT_TEXT)
    {
        if (nloops > 0)
            ExplainPropertyFloat(qlabel, NULL, nfiltered / nloops, 0, es);
        else
            ExplainPropertyFloat(qlabel, NULL, 0.0, 0, es);
    }
}
```