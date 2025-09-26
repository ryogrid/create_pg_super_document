# logicalrep_workers_find

## Location
src/backend/replication/logical/launcher.c: 288 - 312

## Overview  
Searches the logical replication worker array to find all workers associated with a specific subscription, returning them as a list rather than just the first match.

## Definition


## Detailed Description
logicalrep_workers_find performs a comprehensive search through the global logical replication worker array to collect all workers belonging to a specified subscription. Unlike logicalrep_worker_find which returns only the first matching worker, this function builds and returns a complete list of all workers associated with the given subscription ID.

The function iterates through all worker slots (up to max_logical_replication_workers) and examines each worker's subscription ID and status. Unlike its single-result counterpart, this function does not filter by relation ID, making it suitable for operations that need to act on all workers of a subscription regardless of which specific relations they handle.

The function includes the same optional filtering capability for running workers only, and operates under the same locking requirements as logicalrep_worker_find. This makes it particularly useful for subscription-wide operations like cleanup during subscription drops or transaction-end processing.

## Parameters / Member Variables
- : Subscription OID to match against workers' subscription IDs
- : If true, only include workers that are currently running (have an attached process)  
- Returns:  - List of LogicalRepWorker pointers matching the criteria, or NIL if none found

## Dependencies
- Functions called/Symbols referenced:
  - LWLockHeldByMe (for assertion checking)
  - lappend (to build the result list)
- Called from:
  - DropSubscription
  - logicalrep_worker_detach  
  - AtEOXact_LogicalRepWorkers

## Notes and Other Information
- Must be called while holding LogicalRepWorkerLock (enforced by assertion)
- Unlike logicalrep_worker_find, does not skip parallel apply workers - includes all worker types for the subscription
- Does not filter by relation ID, collecting workers for all relations within the subscription
- Used primarily for subscription-wide operations like cleanup and transaction-end processing
- The returned list contains pointers to workers in shared memory, so callers must be careful about memory context and locking
- Critical for ensuring complete cleanup when subscriptions are dropped or during transaction boundaries