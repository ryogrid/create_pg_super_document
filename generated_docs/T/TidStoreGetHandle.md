# TidStoreGetHandle

## Location
[src/backend/access/common/tidstore.c:571-579](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tidstore.c#L571-L579)

## Overview
Returns the DSA handle for a shared TidStore, enabling shared access to the TID store across multiple processes in parallel operations.

## Definition


## Detailed Description
This function retrieves the Dynamic Shared Area (DSA) pointer handle for a shared TidStore. The DSA handle allows multiple processes to access the same TidStore data structure in shared memory. The function includes an assertion to ensure the TidStore is actually shared before attempting to retrieve the handle, preventing misuse with local TidStores.

The function delegates to the internal  function to obtain the actual DSA pointer from the shared radix tree structure.

## Parameters / Member Variables
- : Pointer to the TidStore from which to retrieve the DSA handle. Must be a shared TidStore.

## Dependencies
- Functions called/Symbols referenced:
  - TidStoreIsShared (macro to verify the TidStore is shared)
  - shared_ts_get_handle (internal function to get DSA handle from shared tree)
- Called from (representative examples):
  - [parallel_vacuum_init](../p/parallel_vacuum_init.md) (initializes parallel vacuum workers with shared TidStore)
  - [parallel_vacuum_reset_dead_items](../p/parallel_vacuum_reset_dead_items.md) (resets dead items tracking in parallel vacuum)

## Notes and Other Information
- The function asserts that the TidStore is shared using , which checks if 
- Returns a  type, which is PostgreSQL's handle type for Dynamic Shared Area pointers
- Used primarily in parallel vacuum operations to share TID tracking data between processes
- The returned handle can be passed to other processes to attach to the same shared TidStore