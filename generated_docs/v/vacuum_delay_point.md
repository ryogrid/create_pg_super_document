# vacuum_delay_point

## Location
src/backend/commands/vacuum.c: 2383 - 2481

## Overview
Implements cost-based vacuum delays and interrupt checking to throttle vacuum operations and ensure responsiveness during long-running vacuum processes.

## Definition


## Detailed Description
This function serves as a central control point for vacuum operations, providing both interrupt handling and cost-based delay mechanisms. It should be called in each major loop of VACUUM processing, typically once per page processed. The function implements a sophisticated delay system that can work in both standalone and parallel vacuum scenarios. For parallel vacuum operations, it coordinates with shared cost balances, while for regular vacuum it uses local cost tracking. The function also handles configuration reloading for autovacuum workers and ensures postmaster death detection during extended delays.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS (macro)
  - AmAutoVacuumWorkerProcess
  - ProcessConfigFile
  - VacuumUpdateCosts
  - compute_parallel_delay
  - pgstat_report_wait_start
  - pg_usleep
  - pgstat_report_wait_end
  - PostmasterIsAlive
  - AutoVacuumUpdateCostLimit
- Called from (representative examples):
  - lazy_scan_heap
  - ginbulkdelete
  - gistvacuumpage
  - compute_index_stats
  - acquire_sample_rows

## Notes and Other Information
- Always checks for interrupts first using CHECK_FOR_INTERRUPTS()
- Handles configuration reloading for autovacuum workers when ConfigReloadPending is set
- Implements different delay calculation strategies for parallel vs. non-parallel vacuum
- Caps maximum delay to 4 times the configured vacuum_cost_delay to prevent excessive delays
- Resets VacuumCostBalance to 0 after sleeping
- Includes special handling for postmaster death detection during long delays
- Updates autovacuum cost limits periodically for better load balancing across workers