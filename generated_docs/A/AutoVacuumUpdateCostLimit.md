# AutoVacuumUpdateCostLimit

## Location
[src/backend/postmaster/autovacuum.c:1706-1751](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/autovacuum.c#L1706-L1751)

## Overview
AutoVacuumUpdateCostLimit updates the vacuum_cost_limit for autovacuum workers, implementing load balancing by distributing the total cost limit across active workers while respecting storage parameter overrides.

## Definition
void AutoVacuumUpdateCostLimit(void)

## Detailed Description
AutoVacuumUpdateCostLimit is responsible for calculating and setting the appropriate vacuum cost limit for the current autovacuum worker. The function implements a sophisticated cost balancing system that ensures fair resource distribution among multiple concurrent autovacuum workers. It follows a three-tier hierarchy for determining the base cost limit: storage parameters take highest precedence, followed by autovacuum-specific GUCs, and finally general vacuum settings.

The core balancing mechanism divides the total cost limit by the number of workers requiring balancing, ensuring each worker gets a fair share of the available vacuum resources. Workers with storage parameter overrides are excluded from balancing to honor explicit per-table configurations. The function includes safety checks to prevent invalid cost limits and ensures that each worker gets at least a minimum cost limit of 1.

## Parameters / Member Variables
This function takes no parameters and operates on global state and shared memory variables.

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_unlocked_test_flag](../p/pg_atomic_unlocked_test_flag.md)
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md)
  - Max
  - Assert
  - elog
- Called from (representative examples):
  - [VacuumUpdateCosts](../V/VacuumUpdateCosts.md)
  - [vacuum_delay_point](../v/vacuum_delay_point.md)

## Notes and Other Information
The function only operates on autovacuum workers (identified by MyWorkerInfo being non-NULL) and returns immediately for explicit VACUUM operations. The balancing logic is controlled by the wi_dobalance flag - workers with storage parameter overrides have this flag set to prevent their participation in load balancing. The av_nworkersForBalance counter in shared memory tracks how many workers should participate in cost limit distribution. The function ensures a minimum cost limit of 1 to prevent workers from being completely starved of vacuum resources.