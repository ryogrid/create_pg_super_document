# compute_parallel_delay

## Location
[src/backend/commands/vacuum.c:2482-2525](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuum.c#L2482-L2525)

## Overview
Calculates vacuum delay time for parallel workers based on their proportional share of work, ensuring fair throttling across multiple vacuum workers.

## Definition

```c
static double
compute_parallel_delay(void)
```
## Detailed Description
This function implements a sophisticated cost-based delay mechanism specifically designed for parallel vacuum operations. The core principle is to allow each worker to sleep proportionally to the amount of work (I/O operations) it has performed. The function maintains both shared and local cost balances to track work distribution across workers. It uses atomic operations to safely update shared state and calculates delay time based on the worker's local cost balance relative to the overall system limits. A worker is only put to sleep if it has performed more than 50% of its fair share of work and the overall shared cost balance exceeds the system-wide vacuum cost limit.

## Parameters / Member Variables
- Returns:  - delay time in milliseconds

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md)
  - [pg_atomic_add_fetch_u32](../p/pg_atomic_add_fetch_u32.md)
  - [pg_atomic_sub_fetch_u32](../p/pg_atomic_sub_fetch_u32.md)
  - Assert (macro)
- Called from (representative examples):
  - [vacuum_delay_point](../v/vacuum_delay_point.md)

## Notes and Other Information
- Function is static (internal to vacuum.c)
- Requires VacuumSharedCostBalance to be active (parallel vacuum context)
- Uses atomic operations for thread-safe shared state updates
- Implements a 50% threshold rule: workers sleep only if they've done more than half their fair share
- Resets VacuumCostBalance to 0 after accumulating into shared balance
- Resets VacuumCostBalanceLocal to 0 after computing delay
- The delay calculation is proportional: delay = vacuum_cost_delay * local_balance / vacuum_cost_limit
- Ensures workers doing more I/O are throttled more than those doing less I/O