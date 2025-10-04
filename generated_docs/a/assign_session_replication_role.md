# assign_session_replication_role

## Location
[src/backend/commands/trigger.c:6666-6679](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L6666-L6679)

## Overview
A GUC assign hook function that manages plan cache invalidation when the session_replication_role configuration parameter changes, ensuring trigger behavior changes are properly reflected in cached execution plans.

## Definition

```c
void
assign_session_replication_role(int newval, void *extra)
```
## Detailed Description
This function serves as the assign hook for the session_replication_role Grand Unified Configuration (GUC) parameter in PostgreSQL. The session_replication_role parameter controls how triggers and rules behave in the current session, with different values affecting which triggers fire during statement execution.

The primary responsibility of this function is to maintain cache consistency when the replication role changes. Since execution plans are cached and may include decisions about trigger firing based on the current replication role, changing this setting could make cached plans incorrect or suboptimal. The function addresses this by flushing the entire plan cache when the replication role actually changes.

**Key Behaviors:**

1. **Conditional Cache Flushing**: The function only flushes the plan cache when the new value differs from the current SessionReplicationRole, avoiding unnecessary performance overhead when the value is set to the same value it already had.

2. **Complete Plan Invalidation**: Uses ResetPlanCache() to invalidate all cached execution plans, ensuring that subsequent queries will generate new plans that correctly reflect the new replication role setting.

The session_replication_role parameter typically has three possible values:
-  (default): Normal trigger firing behavior
- : Only fires triggers and rules marked as suitable for replica servers
- : Only fires triggers and rules marked as local

When this setting changes, trigger firing decisions embedded in cached plans may become invalid, making the cache flush essential for correct operation.

## Parameters / Member Variables
- `newval`: The new integer value being assigned to session_replication_role
- `*extra`: Additional data passed to the assign hook (unused in this implementation)
## Dependencies
- Functions called/Symbols referenced:
  - [ResetPlanCache](../R/ResetPlanCache.md) (invalidates all cached execution plans)
  - SessionReplicationRole (global variable containing current replication role)
- Called from (representative examples):
  - GUC system when session_replication_role parameter is modified

## Notes and Other Information
- This function is registered as an assign hook in the GUC (Grand Unified Configuration) system
- The function exemplifies the PostgreSQL pattern of using GUC hooks to maintain system consistency when configuration changes
- [Plan](../P/Plan.md) cache invalidation is necessary because trigger firing decisions may be baked into cached execution plans
- The conditional check prevents unnecessary cache flushes when the parameter is set to its current value, which can happen during configuration reloading
- This function is critical for maintaining correct trigger behavior in replication scenarios where different trigger firing rules may apply
- The function is declared in guc_hooks.h, indicating its role as part of the configuration management infrastructure

## Simplified Source

```c
void assign_session_replication_role(int newval, void *extra)
{
    // Flush plan cache only if replication role actually changed
    if (SessionReplicationRole != newval)
        ResetPlanCache();
}
```