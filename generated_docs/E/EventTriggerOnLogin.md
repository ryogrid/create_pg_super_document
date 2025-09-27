# EventTriggerOnLogin

## Location
[src/backend/commands/event_trigger.c:893-1003](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L893-L1003)

## Overview
EventTriggerOnLogin fires login event triggers when a database connection is established, providing a mechanism for database-level connection monitoring and initialization.

## Definition
```c
void EventTriggerOnLogin(void)
```

## Detailed Description
This function is responsible for firing login event triggers when a new database session is established. Unlike other event trigger functions, it operates at the connection level rather than during DDL operations and includes sophisticated database flag management to optimize connection performance.

Key architectural features:
- Manages its own transaction context using StartTransactionCommand/CommitTransactionCommand
- Requires an active database connection and checks MyDatabaseHasLoginEventTriggers flag for optimization
- Implements intelligent flag management: attempts to clear dathasloginevt when no triggers exist to avoid future unnecessary calls
- Uses conditional locking to prevent connection delays when updating the database flag
- Requires an active snapshot for trigger execution since it may access database objects
- Includes a recheck mechanism to handle concurrent trigger creation/deletion scenarios

The function balances performance optimization (avoiding unnecessary work on subsequent connections) with correctness (properly handling concurrent modifications).

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [EventTriggerData](EventTriggerData.md) (struct for trigger context)
  - [StartTransactionCommand](../S/StartTransactionCommand.md) (begin transaction)
  - [EventTriggerCommonSetup](EventTriggerCommonSetup.md) (identifies applicable triggers, called twice)
  - EVT_Login (event type constant)
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md)/PushActiveSnapshot/PopActiveSnapshot (snapshot management)
  - [EventTriggerInvoke](EventTriggerInvoke.md) (executes the triggers)
  - [ConditionalLockSharedObject](../C/ConditionalLockSharedObject.md) (non-blocking lock acquisition)
  - AccessExclusiveLock (lock type for database metadata)
  - Form_pg_database (database catalog structure)
  - [systable_inplace_update_begin](../s/systable_inplace_update_begin.md)/finish/cancel (atomic flag updates)
  - [heap_freetuple](../h/heap_freetuple.md) (memory cleanup)
  - [list_free](../l/list_free.md) (memory cleanup)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md) (commit transaction)
- Called from (representative examples):
  - [PostgresMain](../P/PostgresMain.md) (database connection setup)

## Notes and Other Information
- Operates within its own transaction context unlike other event trigger functions
- The dathasloginevt flag optimization prevents unnecessary trigger checking on every connection
- Uses conditional locking to avoid hanging connections when clearing the optimization flag
- Implements a double-check pattern to handle race conditions with concurrent trigger modifications
- Uses in-place updates for the database catalog to avoid row-level locking and TOAST complications
- Accepts that concurrent updates may overwrite the flag changes, as subsequent connections can retry
- Part of PostgreSQL's session-level event trigger system for connection monitoring and initialization
- Requires a valid database ID and active postmaster environment

## Simplified Source

```c
// Simplified version of EventTriggerOnLogin
void EventTriggerOnLogin(void) {
    List *trigger_list;
    EventTriggerData trigger_context;

    // Early exit if event triggers are disabled or no database connection
    if (!IsUnderPostmaster || !event_triggers ||
        !OidIsValid(MyDatabaseId) || !MyDatabaseHasLoginEventTriggers)
        return;

    // Start transaction for trigger execution
    StartTransactionCommand();

    // Find all login event triggers that should fire
    trigger_list = EventTriggerCommonSetup(NULL, EVT_Login, "login",
                                          &trigger_context, false);

    if (trigger_list != NIL) {
        // Execute the login triggers with proper snapshot
        PushActiveSnapshot(GetTransactionSnapshot());
        EventTriggerInvoke(trigger_list, &trigger_context);
        PopActiveSnapshot();

        list_free(trigger_list);
    }
    else {
        // No triggers exist - try to clear the database flag to optimize future connections
        if (ConditionalLockSharedObject(DatabaseRelationId, MyDatabaseId, 0, AccessExclusiveLock)) {
            // Double-check no triggers were added concurrently
            trigger_list = EventTriggerCommonSetup(NULL, EVT_Login, "login",
                                                  &trigger_context, true);

            if (trigger_list == NIL) {
                // Clear the dathasloginevt flag in pg_database
                clear_database_login_event_flag();
            } else {
                list_free(trigger_list);
            }
        }
    }

    CommitTransactionCommand();
}

// Helper function (abstracted from complex catalog update logic)
static void clear_database_login_event_flag(void) {
    // Open pg_database catalog and find our database row
    // Update dathasloginevt = false using in-place update
    // Close catalog and cleanup
}
```

Key simplifications made:
- Removed detailed error handling and validation checks for clarity
- Abstracted complex catalog update logic into a helper function
- Consolidated the conditional locking and flag clearing logic
- Simplified variable declarations and memory management details
- Focused on the main execution flow: check conditions → find triggers → execute or optimize
- Preserved the essential double-check pattern for concurrent safety