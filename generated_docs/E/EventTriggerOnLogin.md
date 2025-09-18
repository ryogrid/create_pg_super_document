# EventTriggerOnLogin

## Location
src/backend/commands/event_trigger.c: 893 - 1003

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
- No parameters (operates on the current database session context)

## Dependencies
- Functions called/Symbols referenced:
  - EventTriggerData (struct for trigger context)
  - [StartTransactionCommand](../S/StartTransactionCommand.md) (begin transaction)
  - [EventTriggerCommonSetup](EventTriggerCommonSetup.md) (identifies applicable triggers, called twice)
  - EVT_Login (event type constant)
  - GetTransactionSnapshot/PushActiveSnapshot/PopActiveSnapshot (snapshot management)
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