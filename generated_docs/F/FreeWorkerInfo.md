# FreeWorkerInfo

## Location
[src/backend/postmaster/autovacuum.c:1589-1636](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/autovacuum.c#L1589-L1636)

## Overview
FreeWorkerInfo is a static function that returns an autovacuum worker's WorkerInfo structure to the free list when the worker process terminates, ensuring proper cleanup and resource rebalancing.

## Definition

```c
static void
FreeWorkerInfo(int code, Datum arg)
```
## Detailed Description
FreeWorkerInfo serves as a cleanup callback function that is executed when an autovacuum worker process exits. The function is responsible for returning the worker's WorkerInfo structure to the pool of available workers and triggering a rebalance of the remaining workers' cost limits. It operates under the AutovacuumLock to ensure thread-safe access to shared memory structures.

The function performs several critical cleanup tasks: it removes the worker from the active workers list, resets all worker-specific fields to their default values, adds the worker back to the free workers list, and signals the autovacuum launcher to wake up for potential worker rebalancing. The function also preserves the launcher's PID for signaling purposes, though the actual signal is sent later during process cleanup.

## Parameters / Member Variables
- : Exit code parameter (standard callback parameter, not actively used)
- : Datum argument parameter (standard callback parameter, not actively used)

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire
  - LWLockRelease
  - [dlist_delete](../d/dlist_delete.md)
  - [dlist_push_head](../d/dlist_push_head.md)
  - [pg_atomic_clear_flag](../p/pg_atomic_clear_flag.md)
  - AutoVacRebalance (signal enum value)
- Called from (representative examples):
  - [AutoVacWorkerMain](../A/AutoVacWorkerMain.md)

## Notes and Other Information
This function is designed to be registered as an exit callback, which explains why it follows the standard callback signature with code and arg parameters that are not used in the implementation. The function carefully handles the race condition where the launcher's PID might change between reading and signaling by relying on the process cleanup mechanism to send the actual signal. The rebalancing signal ensures that remaining workers can adjust their cost limits optimally after a worker terminates.