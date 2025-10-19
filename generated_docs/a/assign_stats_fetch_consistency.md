# assign_stats_fetch_consistency

## Location
[src/backend/utils/activity/pgstat.c:1717-1726](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L1717-L1726)

## Overview
The  function serves as a GUC (Grand Unified Configuration) assign hook that handles changes to the  configuration parameter, ensuring statistics snapshot consistency when the parameter value changes.

## Definition

```c
void
assign_stats_fetch_consistency(int newval, void *extra)
```
## Detailed Description
This function is a callback hook that gets invoked whenever the  GUC parameter is modified. Its primary responsibility is to maintain the integrity of statistics snapshots when the consistency level changes during a transaction.

The function implements a defensive mechanism to prevent snapshot state inconsistencies that could arise from changing the fetch consistency mode mid-transaction. When a change is detected, it sets the  flag to true, which ensures that the current statistics snapshot will be cleared on the next snapshot build attempt.

This approach prevents situations where a transaction might see inconsistent statistics data due to changing consistency requirements during execution, which could lead to unpredictable query planning and execution behavior.

## Parameters / Member Variables
- `newval`: The new value being assigned to the  GUC parameter
- `*extra`: Additional context data passed by the GUC system (unused in this implementation)
## Dependencies
- Functions called/Symbols referenced:
  - pgstat_fetch_consistency: Global variable holding the current fetch consistency level
  - force_stats_snapshot_clear: Global flag that triggers snapshot clearing

- Called from (representative examples):
  - GUC system: Invoked automatically when the  parameter is changed
  - Referenced in: src/include/utils/guc_hooks.h:137

## Notes and Other Information
- This is a GUC assign hook function, part of PostgreSQL's configuration management system
- The function prevents snapshot inconsistencies that could occur when changing consistency modes during active transactions
- Setting  ensures that stale snapshot data doesn't persist with the new consistency setting
- The hook pattern allows for validation and side effects when configuration parameters change
- Critical for maintaining data consistency in PostgreSQL's statistics subsystem
- The extra parameter follows the standard GUC hook signature but is not used in this implementation
- Located in src/backend/utils/activity/pgstat.c:1717-1726

## Simplified Source

```c
void
assign_stats_fetch_consistency(int newval, void *extra)
{
    // Check if the consistency setting is actually changing
    if (pgstat_fetch_consistency != newval)
    {
        // Force clear of current snapshot to prevent inconsistencies
        force_stats_snapshot_clear = true;
    }
}
```