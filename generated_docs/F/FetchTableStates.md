# FetchTableStates

## Location
src/backend/replication/logical/tablesync.c: 1598 - 1668

## Overview
FetchTableStates retrieves and caches the current synchronization state information for all tables in a subscription, maintaining an up-to-date view of tables requiring synchronization work.

## Definition
```c
static bool FetchTableStates(bool *started_tx)
```

## Detailed Description
This function serves as the central mechanism for maintaining cached information about table synchronization states in logical replication. It implements a validity-based caching system that rebuilds the table state information only when necessary, optimizing performance by avoiding redundant database queries.

The function operates with a sophisticated caching mechanism using a validity flag system. When the cached information is marked as invalid (not SYNC_TABLE_STATE_VALID), it triggers a complete rebuild of the table states list. The rebuild process involves:

1. **Cache Invalidation**: Clearing existing cached table state information
2. **Transaction Management**: Starting a transaction if none is active (with proper caller notification)
3. **Data Retrieval**: Fetching all non-ready subscription relations from the system catalogs
4. **Memory Management**: Allocating tracking structures in permanent memory context for persistence
5. **Validation**: Determining if the subscription has any tables at all
6. **Consistency Protection**: Handling concurrent cache invalidations gracefully

The function uses static variables to maintain state across calls, implementing a form of session-level caching that persists until explicitly invalidated.

## Parameters / Member Variables
- `started_tx`: Output parameter that indicates whether this function started a new transaction (true) or used an existing one (false). The caller must commit the transaction if this flag is set to true.

## Dependencies
- Functions called/Symbols referenced:
  - [IsTransactionState](../I/IsTransactionState.md)
  - [StartTransactionCommand](../S/StartTransactionCommand.md)
  - [GetSubscriptionRelations](../G/GetSubscriptionRelations.md)
  - [HasSubscriptionRelations](../H/HasSubscriptionRelations.md)
  - [list_free_deep](../l/list_free_deep.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
- Called from (representative examples):
  - SyncingTablesState
  - [tablesync_start_time_mapping](../t/tablesync_start_time_mapping.md)
  - [AllTablesyncsReady](../A/AllTablesyncsReady.md)

## Notes and Other Information
- Uses static variables (has_subrels, table_states_validity) to maintain cache state across function calls
- Implements a three-state validity system: VALID, REBUILD_STARTED, and invalid (implicitly)
- Allocates cached data in CacheMemoryContext for persistence across memory context resets
- Handles concurrent invalidation gracefully by allowing in-progress rebuilds to complete while marking for future rebuild
- The function distinguishes between subscriptions with no tables vs. subscriptions with only ready tables
- Critical for performance as it prevents repeated expensive catalog queries during table sync operations
- Transaction management is carefully designed to work within existing transaction contexts or create new ones as needed