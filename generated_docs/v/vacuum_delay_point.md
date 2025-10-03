# vacuum_delay_point

## Location
[src/backend/commands/vacuum.c:2383-2481](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuum.c#L2383-L2481)

## Overview
Implements cost-based vacuum delays and interrupt checking to throttle vacuum operations and ensure responsiveness during long-running vacuum processes.

## Definition

```c
void
vacuum_delay_point(void)
```
## Detailed Description
This function serves as a central control point for vacuum operations, providing both interrupt handling and cost-based delay mechanisms. It should be called in each major loop of VACUUM processing, typically once per page processed. The function implements a sophisticated delay system that can work in both standalone and parallel vacuum scenarios. For parallel vacuum operations, it coordinates with shared cost balances, while for regular vacuum it uses local cost tracking. The function also handles configuration reloading for autovacuum workers and ensures postmaster death detection during extended delays.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS (macro)
  - AmAutoVacuumWorkerProcess
  - ProcessConfigFile
  - [VacuumUpdateCosts](../V/VacuumUpdateCosts.md)
  - [compute_parallel_delay](../c/compute_parallel_delay.md)
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)
  - [pg_usleep](../p/pg_usleep.md)
  - [pgstat_report_wait_end](../p/pgstat_report_wait_end.md)
  - [PostmasterIsAlive](../P/PostmasterIsAlive.md)
  - [AutoVacuumUpdateCostLimit](../A/AutoVacuumUpdateCostLimit.md)
- Called from (representative examples):
  - [lazy_scan_heap](../l/lazy_scan_heap.md)
  - [ginbulkdelete](../g/ginbulkdelete.md)
  - [gistvacuumpage](../g/gistvacuumpage.md)
  - [compute_index_stats](../c/compute_index_stats.md)
  - [acquire_sample_rows](../a/acquire_sample_rows.md)

## Notes and Other Information
- Always checks for interrupts first using CHECK_FOR_INTERRUPTS()
- Handles configuration reloading for autovacuum workers when ConfigReloadPending is set
- Implements different delay calculation strategies for parallel vs. non-parallel vacuum
- Caps maximum delay to 4 times the configured vacuum_cost_delay to prevent excessive delays
- Resets VacuumCostBalance to 0 after sleeping
- Includes special handling for postmaster death detection during long delays
- Updates autovacuum cost limits periodically for better load balancing across workers

## Simplified Source

```c
void vacuum_delay_point(void)
{
    double msec = 0;

    // Always check for interrupts first
    CHECK_FOR_INTERRUPTS();

    // Early return if no delays needed or interrupts pending
    if (InterruptPending || (!VacuumCostActive && !ConfigReloadPending))
        return;

    // Handle config reload for autovacuum workers
    if (ConfigReloadPending && AmAutoVacuumWorkerProcess()) {
        ConfigReloadPending = false;
        ProcessConfigFile(PGC_SIGHUP);
        VacuumUpdateCosts();
    }

    // Exit if cost-based delays were disabled after config reload
    if (!VacuumCostActive)
        return;

    // Calculate delay time based on cost balance
    if (VacuumSharedCostBalance != NULL) {
        // Parallel vacuum - use shared cost balance
        msec = compute_parallel_delay();
    } else if (VacuumCostBalance >= vacuum_cost_limit) {
        // Regular vacuum - calculate proportional delay
        msec = vacuum_cost_delay * VacuumCostBalance / vacuum_cost_limit;
    }

    // Apply delay if needed
    if (msec > 0) {
        // Cap maximum delay to prevent excessive waits
        if (msec > vacuum_cost_delay * 4)
            msec = vacuum_cost_delay * 4;

        // Sleep with wait event reporting
        pgstat_report_wait_start(WAIT_EVENT_VACUUM_DELAY);
        pg_usleep(msec * 1000);
        pgstat_report_wait_end();

        // Check for postmaster death during long delays
        if (IsUnderPostmaster && !PostmasterIsAlive())
            exit(1);

        // Reset cost balance and update autovacuum limits
        VacuumCostBalance = 0;
        AutoVacuumUpdateCostLimit();

        // Check for interrupts after sleeping
        CHECK_FOR_INTERRUPTS();
    }
}
```