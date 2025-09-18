# VacuumUpdateCosts

## Location
[src/backend/postmaster/autovacuum.c:1637-1705](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/autovacuum.c#L1637-L1705)

## Overview
VacuumUpdateCosts updates vacuum cost-based delay parameters for autovacuum workers and explicit VACUUM/ANALYZE operations, ensuring current GUC values are applied and cost-based throttling is properly configured.

## Definition
void VacuumUpdateCosts(void)

## Detailed Description
VacuumUpdateCosts is a critical function that synchronizes vacuum cost parameters with current configuration values. It handles two distinct execution contexts: autovacuum workers and explicit VACUUM/ANALYZE commands. For autovacuum workers, it applies a hierarchy of cost delay settings, checking storage parameters first, then autovacuum-specific GUCs, and finally falling back to general vacuum settings. The function also manages the VacuumCostActive flag based on delay configuration and failsafe state.

The function includes comprehensive debug logging for autovacuum operations, providing visibility into cost parameter decisions and current worker state. It carefully manages lock acquisition for shared memory access and ensures that configuration changes are properly reflected in the active vacuum cost system. The cost limit calculation for autovacuum workers is delegated to AutoVacuumUpdateCostLimit for proper load balancing.

## Parameters / Member Variables
This function takes no parameters and operates on global state variables.

## Dependencies
- Functions called/Symbols referenced:
  - [AutoVacuumUpdateCostLimit](../A/AutoVacuumUpdateCostLimit.md)
  - [message_level_is_interesting](../m/message_level_is_interesting.md)
  - LWLockHeldByMe
  - LWLockAcquire
  - LWLockRelease
  - [pg_atomic_unlocked_test_flag](../p/pg_atomic_unlocked_test_flag.md)
  - elog
  - Assert
- Called from (representative examples):
  - vacuum (from vacuum.c)
  - [vacuum_delay_point](../v/vacuum_delay_point.md)
  - [parallel_vacuum_main](../p/parallel_vacuum_main.md)
  - [do_autovacuum](../d/do_autovacuum.md)

## Notes and Other Information
The function differentiates between autovacuum workers and explicit vacuum operations through the MyWorkerInfo global variable. For autovacuum workers, cost parameters follow a three-tier hierarchy: storage parameters override autovacuum GUCs, which override general vacuum GUCs. The debug logging is conditionally compiled to avoid lock overhead when DEBUG2 logging is not enabled. The function ensures that VacuumCostActive and VacuumFailsafeActive states are mutually exclusive, with failsafe mode disabling cost-based delays.