# autovac_recalculate_workers_for_balance

## Location
src/backend/postmaster/autovacuum.c: 1752 - 1791

## Overview
autovac_recalculate_workers_for_balance recalculates the number of active autovacuum workers that should participate in cost limit balancing, excluding workers with storage parameter overrides.

## Definition
static void autovac_recalculate_workers_for_balance(void)

## Detailed Description
autovac_recalculate_workers_for_balance is a critical function for maintaining accurate cost limit distribution among autovacuum workers. It iterates through all currently running workers and counts those eligible for cost balancing, excluding workers that have cost-related storage parameters configured or are not properly initialized. The function updates the shared memory counter av_nworkersForBalance only when the count has changed, minimizing unnecessary atomic operations.

The function serves as the authoritative source for determining how many workers should share the total autovacuum cost limit. Workers are excluded from balancing if they have NULL process pointers (indicating they're not fully initialized) or if their wi_dobalance flag is set (indicating they have storage parameter overrides). This ensures that explicitly configured tables maintain their intended resource allocation while allowing other workers to share the remaining resources fairly.

## Parameters / Member Variables
This function takes no parameters and operates on shared memory structures.

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_iter](../d/dlist_iter.md)
  - LWLockHeldByMe
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md)
  - dlist_foreach
  - dlist_container
  - [pg_atomic_unlocked_test_flag](../p/pg_atomic_unlocked_test_flag.md)
  - [pg_atomic_write_u32](../p/pg_atomic_write_u32.md)
  - Assert
  - [WorkerInfoData](../W/WorkerInfoData.md) (struct type)
  - [WorkerInfo](../W/WorkerInfo.md) (typedef)
- Called from (representative examples):
  - [do_autovacuum](../d/do_autovacuum.md)

## Notes and Other Information
The function must be called with AutovacuumLock held in at least shared mode, as documented in the function comment and enforced by the Assert statement. The atomic operations ensure thread-safe access to the shared counter without requiring exclusive locks. The function's design minimizes performance impact by only updating the shared counter when the worker count actually changes, avoiding unnecessary cache invalidation in multi-worker scenarios. The wi_dobalance flag mechanism allows fine-grained control over which workers participate in cost balancing.