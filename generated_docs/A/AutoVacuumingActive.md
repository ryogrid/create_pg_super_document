# AutoVacuumingActive

## Location
[src/backend/postmaster/autovacuum.c:3233-3244](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/autovacuum.c#L3233-L3244)

## Overview
Determines whether the autovacuum system should be active by checking essential GUC configuration parameters.

## Definition

```c
bool
AutoVacuumingActive(void)
```
## Detailed Description
This function serves as a centralized check to determine if the autovacuum system should be operational. It validates two critical prerequisites for autovacuum functionality:

1. **Daemon Enablement**: Checks that  GUC parameter is enabled
2. **Statistics Collection**: Verifies that  is enabled, which is essential for autovacuum decision-making

The function returns  only when both conditions are met, ensuring that autovacuum operations are performed only when the system is properly configured. This prevents autovacuum from running in environments where it cannot function correctly due to missing statistics or explicit disabling.

The function is used throughout the autovacuum system to gate operations that should only occur when autovacuum is properly configured and enabled.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - autovacuum_start_daemon (GUC variable)
  - pgstat_track_counts (GUC variable)
- Called from (representative examples):
  - [HandleAutoVacLauncherInterrupts](../H/HandleAutoVacLauncherInterrupts.md)
  - [relation_needs_vacanalyze](../r/relation_needs_vacanalyze.md)
  - [ServerLoop](../S/ServerLoop.md)
  - [process_pm_child_exit](../p/process_pm_child_exit.md)

## Notes and Other Information
- This is a simple but critical gatekeeper function for the entire autovacuum system
- The check for  is essential because autovacuum relies on table statistics to make decisions
- Used by both the autovacuum launcher and worker processes to ensure proper configuration
- Also used by the postmaster process to determine whether autovacuum processes should be started
- Returns  immediately if either prerequisite is not met, using short-circuit evaluation

## Simplified Source

```c
// Simplified version of AutoVacuumingActive
bool AutoVacuumingActive(void) {
    // Check if autovacuum daemon is enabled AND statistics tracking is enabled
    if (!autovacuum_start_daemon || !pgstat_track_counts) {
        return false;  // Autovacuum cannot run without both prerequisites
    }

    // Both conditions met - autovacuum can be active
    return true;
}
```

Key simplifications made:
- Added descriptive comments explaining the core logic
- Emphasized the dual prerequisite check
- Made the conditional logic more explicit with explanatory comments
- Highlighted that both conditions must be true for autovacuum to be active