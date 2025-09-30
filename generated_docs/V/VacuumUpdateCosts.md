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
  - [LWLockHeldByMe](../L/LWLockHeldByMe.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - [pg_atomic_unlocked_test_flag](../p/pg_atomic_unlocked_test_flag.md)
  - elog
  - Assert
- Called from (representative examples):
  - [vacuum](../v/vacuum.md) (from vacuum.c)
  - [vacuum_delay_point](../v/vacuum_delay_point.md)
  - [parallel_vacuum_main](../p/parallel_vacuum_main.md)
  - [do_autovacuum](../d/do_autovacuum.md)

## Notes and Other Information
The function differentiates between autovacuum workers and explicit vacuum operations through the MyWorkerInfo global variable. For autovacuum workers, cost parameters follow a three-tier hierarchy: storage parameters override autovacuum GUCs, which override general vacuum GUCs. The debug logging is conditionally compiled to avoid lock overhead when DEBUG2 logging is not enabled. The function ensures that VacuumCostActive and VacuumFailsafeActive states are mutually exclusive, with failsafe mode disabling cost-based delays.

## Simplified Source

```c
void VacuumUpdateCosts(void) {
    if (MyWorkerInfo) {
        // Autovacuum worker: apply cost delay hierarchy
        if (av_storage_param_cost_delay >= 0)
            vacuum_cost_delay = av_storage_param_cost_delay;
        else if (autovacuum_vac_cost_delay >= 0)
            vacuum_cost_delay = autovacuum_vac_cost_delay;
        else
            vacuum_cost_delay = VacuumCostDelay;  // fallback

        // Update cost limit for load balancing
        AutoVacuumUpdateCostLimit();
    } else {
        // Explicit VACUUM/ANALYZE: use global settings
        vacuum_cost_delay = VacuumCostDelay;
        vacuum_cost_limit = VacuumCostLimit;
    }

    // Update cost-based throttling state
    if (VacuumFailsafeActive) {
        Assert(!VacuumCostActive);
    } else if (vacuum_cost_delay > 0) {
        VacuumCostActive = true;
    } else {
        VacuumCostActive = false;
        VacuumCostBalance = 0;
    }

    // Debug logging for autovacuum workers
    if (MyWorkerInfo && message_level_is_interesting(DEBUG2)) {
        Oid dboid, tableoid;

        LWLockAcquire(AutovacuumLock, LW_SHARED);
        dboid = MyWorkerInfo->wi_dboid;
        tableoid = MyWorkerInfo->wi_tableoid;
        LWLockRelease(AutovacuumLock);

        elog(DEBUG2,
             "Autovacuum VacuumUpdateCosts(db=%u, rel=%u, dobalance=%s, "
             "cost_limit=%d, cost_delay=%g active=%s failsafe=%s)",
             dboid, tableoid,
             pg_atomic_unlocked_test_flag(&MyWorkerInfo->wi_dobalance) ? "no" : "yes",
             vacuum_cost_limit, vacuum_cost_delay,
             vacuum_cost_delay > 0 ? "yes" : "no",
             VacuumFailsafeActive ? "yes" : "no");
    }
}
```