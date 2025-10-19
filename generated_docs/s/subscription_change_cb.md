# subscription_change_cb

## Location
[src/backend/replication/logical/worker.c:4004-4018](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L4004-L4018)

## Overview
A callback function triggered by subscription syscache invalidation to mark the current subscription as invalid.

## Definition

```c
struct the subxact filename */
	subxact_filename(path, subid, xid);
```
## Detailed Description
This function serves as a callback that is invoked when the subscription system cache is invalidated. Its primary purpose is to set the global flag  to false, indicating that the cached subscription information is no longer valid and needs to be refreshed. This is a critical part of PostgreSQL's logical replication infrastructure, ensuring that subscription workers respond appropriately to changes in subscription configuration.

## Parameters / Member Variables
- : Datum argument passed to the callback (unused in this implementation)
- : Cache identifier indicating which cache was invalidated
- : Hash value associated with the invalidated cache entry

## Dependencies
- Functions called/Symbols referenced:
  - MySubscriptionValid (global variable)
- Called from (representative examples):
  - [InitializeLogRepWorker](../I/InitializeLogRepWorker.md)

## Notes and Other Information
- This is a static function, meaning it has internal linkage and is only visible within the worker.c compilation unit
- The function is registered as a callback with the subscription syscache invalidation system
- Setting MySubscriptionValid to false triggers subscription information to be reloaded when next accessed
- This mechanism ensures consistency between subscription workers and the subscription catalog

## Simplified Source

```c
/*
 * Callback from subscription syscache invalidation.
 */
static void
subscription_change_cb(Datum arg, int cacheid, uint32 hashvalue)
{
    // Mark cached subscription data as invalid
    MySubscriptionValid = false;
}
```