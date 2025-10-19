# should_apply_changes_for_rel

## Location
[src/backend/replication/logical/worker.c:470-509](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L470-L509)

## Overview
Determines whether a logical replication worker should apply changes for a given relation based on worker type and relation synchronization state.

## Definition
```c
static bool should_apply_changes_for_rel(LogicalRepRelMapEntry *rel)
```

## Detailed Description
This function implements the core decision logic for whether to apply replication changes to a specific relation. It handles different worker types with distinct behaviors:

- **TABLESYNC workers**: Only apply changes for the specific table they are synchronizing (matching relid)
- **PARALLEL_APPLY workers**: Only apply to relations in READY state, with error handling for unsupported states during streaming transactions
- **APPLY workers**: Apply to READY relations or SYNCDONE relations where the state LSN is within the acceptable range

The function is critical for coordinating parallel table synchronization and ensuring changes are applied by the appropriate worker at the right time.

## Parameters / Member Variables
- `rel`: Pointer to LogicalRepRelMapEntry containing relation mapping information including state and LSN tracking

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalRepRelMapEntry](../L/LogicalRepRelMapEntry.md) (relation mapping structure)
  - WORKERTYPE_TABLESYNC, WORKERTYPE_PARALLEL_APPLY, WORKERTYPE_APPLY, WORKERTYPE_UNKNOWN (worker type constants)
  - SUBREL_STATE_READY, SUBREL_STATE_UNKNOWN, SUBREL_STATE_SYNCDONE (relation state constants)
  - MyLogicalRepWorker (global worker state)
  - MySubscription (global subscription info)
  - remote_final_lsn (transaction LSN tracking)
  - ereport, elog (error reporting functions)
- Called from (representative examples):
  - [apply_handle_insert](../a/apply_handle_insert.md) (INSERT operation processing)
  - [apply_handle_update](../a/apply_handle_update.md) (UPDATE operation processing)
  - [apply_handle_delete](../a/apply_handle_delete.md) (DELETE operation processing)
  - [apply_handle_truncate](../a/apply_handle_truncate.md) (TRUNCATE operation processing)

## Notes and Other Information
- Uses <= comparison for SYNCDONE state because the state LSN might hold the position of the end of initial slot consistent point + 1
- Parallel apply workers require all tables to be in READY state before handling streamed transactions
- The function includes safeguards against race conditions during ALTER SUBSCRIPTION ... REFRESH PUBLICATION
- TABLESYNC workers are restricted to their assigned table to prevent interference with parallel synchronization
- Error handling prevents parallel workers from proceeding with unsynchronized tables

## Simplified Source

```c
/*
 * Should this worker apply changes for given relation.
 *
 * This is mainly needed for initial relation data sync as that runs in
 * separate worker process running in parallel and we need some way to skip
 * changes coming to the leader apply worker during the sync of a table.
 */
static bool
should_apply_changes_for_rel(LogicalRepRelMapEntry *rel)
{
    switch (MyLogicalRepWorker->type)
    {
        case WORKERTYPE_TABLESYNC:
            // Table sync workers only apply to their assigned table
            return MyLogicalRepWorker->relid == rel->localreloid;

        case WORKERTYPE_PARALLEL_APPLY:
            // Parallel workers only handle ready relations
            if (rel->state != SUBREL_STATE_READY &&
                rel->state != SUBREL_STATE_UNKNOWN)
                ereport(ERROR,
                        errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("logical replication parallel apply worker for subscription \"%s\" will stop",
                               MySubscription->name),
                        errdetail("Cannot handle streamed replication transactions using parallel apply workers until all tables have been synchronized."));

            return rel->state == SUBREL_STATE_READY;

        case WORKERTYPE_APPLY:
            // Apply workers handle ready tables or synced tables within LSN range
            return (rel->state == SUBREL_STATE_READY ||
                    (rel->state == SUBREL_STATE_SYNCDONE &&
                     rel->statelsn <= remote_final_lsn));

        case WORKERTYPE_UNKNOWN:
            elog(ERROR, "Unknown worker type");
    }

    return false;
}
```