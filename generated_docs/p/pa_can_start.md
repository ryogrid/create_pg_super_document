# pa_can_start

## Location
[src/backend/replication/logical/applyparallelworker.c:265-326](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/applyparallelworker.c#L265-L326)

## Overview
Determines whether it is safe to start a parallel apply worker in PostgreSQL logical replication.

## Definition

```c
static bool
pa_can_start(void)
```
## Detailed Description
This function performs a series of checks to determine if a parallel apply worker can be started safely. It ensures that only leader apply workers can start parallel workers and validates various subscription and replication state conditions. The function is crucial for maintaining the integrity of parallel logical replication by preventing worker creation when it would be inappropriate or unsafe.

## Parameters / Member Variables
(No parameters - void function)

## Dependencies
- Functions called/Symbols referenced:
  - [am_leader_apply_worker](../a/am_leader_apply_worker.md)
  - [maybe_reread_subscription](../m/maybe_reread_subscription.md)
  - XLogRecPtrIsInvalid
  - [AllTablesyncsReady](../A/AllTablesyncsReady.md)
- Called from:
  - [pa_allocate_worker](pa_allocate_worker.md)

## Notes and Other Information
- Only leader apply workers can start parallel apply workers
- Checks subscription parameters to ensure they support parallel streaming
- Prevents parallel worker creation when skiplsn is set, as streaming transactions need to be serialized for LSN comparison
- Ensures all table synchronizations are ready before allowing parallel workers, as remote_final_lsn determination is required for applying changes to relations not in READY state
- Part of PostgreSQL's logical replication parallel processing system located in src/backend/replication/logical/applyparallelworker.c:265-326