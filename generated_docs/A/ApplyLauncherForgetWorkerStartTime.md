# ApplyLauncherForgetWorkerStartTime

## Location
[src/backend/replication/logical/launcher.c:1088-1098](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L1088-L1098)

## Overview
Removes the last-start-time entry for a subscription from the shared hash table, allowing immediate worker restart and preventing memory leaks for deleted subscriptions.

## Definition
```c
void ApplyLauncherForgetWorkerStartTime(Oid subid)
```

## Detailed Description
This function deletes the start time record for a specific subscription from the shared hash table. It serves two primary purposes: cleanup of entries for deleted or disabled subscriptions to prevent shared memory leaks, and enabling immediate restart of workers that have exited due to subscription parameter changes (bypassing normal restart throttling).

The function is straightforward - it ensures the shared hash table is accessible and then uses `dshash_delete_key()` to remove the entry. The return value of `dshash_delete_key()` is explicitly ignored with a void cast, indicating that the function doesn't care whether the entry actually existed or not.

This cleanup mechanism is essential for subscription lifecycle management and allows the system to distinguish between failed workers (which should be throttled) and workers that exited cleanly due to configuration changes (which can restart immediately).

## Parameters / Member Variables
- `subid`: The OID of the subscription for which to remove the start time entry

## Dependencies
- Functions called/Symbols referenced:
  - [logicalrep_launcher_attach_dshmem](../l/logicalrep_launcher_attach_dshmem.md)
  - [dshash_delete_key](../d/dshash_delete_key.md)
- Called from:
  - [DropSubscription](../D/DropSubscription.md)
  - [tablesync_start_time_mapping](../t/tablesync_start_time_mapping.md)  
  - [apply_worker_exit](../a/apply_worker_exit.md)
  - [maybe_reread_subscription](../m/maybe_reread_subscription.md)
  - [InitializeLogRepWorker](../I/InitializeLogRepWorker.md)
  - [DisableSubscriptionAndExit](../D/DisableSubscriptionAndExit.md)

## Notes and Other Information
- This is a public function (not static) and is exported in logicallauncher.h
- The function gracefully handles cases where no entry exists for the subscription
- Used extensively throughout the logical replication system for proper cleanup
- Critical for preventing shared memory leaks when subscriptions are modified or deleted
- Enables immediate worker restart by removing throttling constraints for configuration changes
- The void cast on `dshash_delete_key()` indicates deliberate ignoring of whether the key existed

## Simplified Source

```c
void ApplyLauncherForgetWorkerStartTime(Oid subid)
{
    // Ensure shared memory hash table is accessible
    logicalrep_launcher_attach_dshmem();

    // Remove start time entry for subscription (ignore if not found)
    (void) dshash_delete_key(last_start_times, &subid);
}
```