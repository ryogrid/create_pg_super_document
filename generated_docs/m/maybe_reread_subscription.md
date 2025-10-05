# maybe_reread_subscription

## Location
[src/backend/replication/logical/worker.c:3875-4003](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L3875-L4003)

## Overview
maybe_reread_subscription checks for changes in subscription configuration and triggers appropriate worker actions including restart or termination when parameters have changed.

## Definition

```c
struct the subxact filename */
	subxact_filename(path, subid, xid);
```
## Detailed Description
This function validates and updates the current subscription configuration by comparing the cached subscription data with the current state in the system catalog. It handles various scenarios including subscription removal, disabling, and parameter changes that require worker restart. The function manages transaction state appropriately, ensuring proper memory context usage and configuration updates. When significant changes are detected (connection parameters, publications, owner privileges), it triggers worker exit to allow the launcher to restart with updated configuration. It also handles synchronous_commit setting changes and validates that critical parameters like database ID haven't changed unexpectedly.

## Parameters / Member Variables
None - This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [IsTransactionState](../I/IsTransactionState.md)
  - [StartTransactionCommand](../S/StartTransactionCommand.md)
  - [GetSubscription](../G/GetSubscription.md)
  - [ApplyLauncherForgetWorkerStartTime](../A/ApplyLauncherForgetWorkerStartTime.md)
  - [apply_worker_exit](../a/apply_worker_exit.md)
  - [FreeSubscription](../F/FreeSubscription.md)
  - [SetConfigOption](../S/SetConfigOption.md)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md)
- Called from (representative examples):
  - [LogicalRepApplyLoop](../L/LogicalRepApplyLoop.md) (at line 3638)
  - [apply_handle_commit_internal](../a/apply_handle_commit_internal.md) (at line 2288)
  - [begin_replication_step](../b/begin_replication_step.md) (at line 517)
  - [pa_can_start](../p/pa_can_start.md) (in applyparallelworker.c at line 280)

## Notes and Other Information
- This is a public function (not static) that can be called from other modules
- Uses MySubscriptionValid flag to avoid unnecessary work when cache is current
- Handles transaction management by starting/committing transactions as needed
- Compares multiple subscription parameters: conninfo, name, slotname, binary, stream, passwordrequired, origin, owner, publications
- Validates that critical parameters like twophasestate and dbid haven't changed unexpectedly
- Different log messages for parallel vs regular apply workers
- Updates synchronous_commit configuration when subscription changes
- Memory management uses ApplyContext for permanent allocations
- Exits cleanly on subscription removal or disabling to prevent resource leaks

## Simplified Source

```c
void maybe_reread_subscription(void) {
    MemoryContext oldctx;
    Subscription *newsub;
    bool started_tx = false;

    // Skip if cache is still valid
    if (MySubscriptionValid)
        return;

    // Start transaction if not already in one
    if (!IsTransactionState()) {
        StartTransactionCommand();
        started_tx = true;
    }

    // Switch to permanent memory context
    oldctx = MemoryContextSwitchTo(ApplyContext);

    // Get current subscription configuration
    newsub = GetSubscription(MyLogicalRepWorker->subid, true);

    // Exit if subscription was removed
    if (!newsub) {
        ereport(LOG, (errmsg("subscription removed, stopping worker")));
        if (am_leader_apply_worker())
            ApplyLauncherForgetWorkerStartTime(MyLogicalRepWorker->subid);
        proc_exit(0);
    }

    // Exit if subscription was disabled
    if (!newsub->enabled) {
        ereport(LOG, (errmsg("subscription disabled, stopping worker")));
        apply_worker_exit();
    }

    // Check for parameter changes that require restart
    if (strcmp(newsub->conninfo, MySubscription->conninfo) != 0 ||
        strcmp(newsub->name, MySubscription->name) != 0 ||
        strcmp(newsub->slotname, MySubscription->slotname) != 0 ||
        newsub->binary != MySubscription->binary ||
        newsub->stream != MySubscription->stream ||
        newsub->passwordrequired != MySubscription->passwordrequired ||
        strcmp(newsub->origin, MySubscription->origin) != 0 ||
        newsub->owner != MySubscription->owner ||
        !equal(newsub->publications, MySubscription->publications)) {

        ereport(LOG, (errmsg("parameter change detected, restarting worker")));
        apply_worker_exit();
    }

    // Exit if owner lost superuser privileges
    if (!newsub->ownersuperuser && MySubscription->ownersuperuser) {
        ereport(LOG, (errmsg("owner privileges revoked, restarting worker")));
        apply_worker_exit();
    }

    // Validate critical parameters haven't changed unexpectedly
    if (newsub->dbid != MySubscription->dbid) {
        elog(ERROR, "subscription %u changed unexpectedly", MyLogicalRepWorker->subid);
    }

    // Update subscription and restore memory context
    FreeSubscription(MySubscription);
    MySubscription = newsub;
    MemoryContextSwitchTo(oldctx);

    // Update synchronous commit setting
    SetConfigOption("synchronous_commit", MySubscription->synccommit,
                    PGC_BACKEND, PGC_S_OVERRIDE);

    // Commit transaction if we started it
    if (started_tx)
        CommitTransactionCommand();

    MySubscriptionValid = true;
}
```