# IdentifySystem

## Location
[src/backend/replication/walsender.c:411-493](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L411-L493)

## Overview
IdentifySystem handles the IDENTIFY_SYSTEM replication command by returning system identification information including system ID, timeline ID, current WAL position, and database name in a single-row result set.

## Definition
```c
static void IdentifySystem(void)
```

## Detailed Description
IdentifySystem is a static function that implements the IDENTIFY_SYSTEM replication protocol command. This command is typically the first command sent by replication clients to establish the identity and current state of the PostgreSQL server they're connecting to. The function creates a result set with four columns containing critical replication information.

The function determines whether the server is in recovery mode (cascading WAL sender) and retrieves the appropriate WAL flush position accordingly. For servers in recovery, it gets the standby flush position; otherwise, it gets the regular flush position. If connected to a specific database, it retrieves the database name using a temporary transaction context since syscache access requires a transaction environment.

The function constructs a tuple descriptor with four columns and sends a single tuple containing the system identifier, current timeline ID, WAL position formatted as a string, and database name (or NULL if not connected to a specific database).

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [GetSystemIdentifier](../G/GetSystemIdentifier.md)
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - [GetStandbyFlushRecPtr](../G/GetStandbyFlushRecPtr.md)
  - [GetFlushRecPtr](../G/GetFlushRecPtr.md)
  - [StartTransactionCommand](../S/StartTransactionCommand.md)
  - [get_database_name](../g/get_database_name.md)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md)
  - [CreateDestReceiver](../C/CreateDestReceiver.md)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)
  - [TupleDescInitBuiltinEntry](../T/TupleDescInitBuiltinEntry.md)
  - [begin_tup_output_tupdesc](../b/begin_tup_output_tupdesc.md)
  - [Int64GetDatum](Int64GetDatum.md)
  - [do_tup_output](../d/do_tup_output.md)
  - [end_tup_output](../e/end_tup_output.md)
  - DestRemoteSimple
  - MAXFNAMELEN
  - UINT64_FORMAT

- Called from:
  - [exec_replication_command](../e/exec_replication_command.md) (when processing IDENTIFY_SYSTEM command)

## Notes and Other Information
- This is a static function only accessible within walsender.c
- The function handles both regular and cascading WAL sender scenarios differently for WAL position retrieval
- Uses temporary transaction context for database name lookup since syscache access requires transaction environment
- Returns a result set with exactly four columns: systemid (text), timeline (int8), xlogpos (text), dbname (text or NULL)
- The WAL position is formatted as a string in PostgreSQL's standard LSN format (XXX/XXX)
- Database name is set to NULL if not connected to a specific database (MyDatabaseId == InvalidOid)
- This command is fundamental to PostgreSQL's streaming replication protocol and is typically the first command executed by replication clients
- The system identifier uniquely identifies a PostgreSQL cluster and is used to prevent replication between incompatible systems

## Simplified Source

```c
// Simplified version of IdentifySystem
static void IdentifySystem(void) {
    char system_id[32];
    char wal_location[MAXFNAMELEN];
    XLogRecPtr log_position;
    char *database_name = NULL;
    TimeLineID current_timeline;

    // Get system identifier as string
    snprintf(system_id, sizeof(system_id), UINT64_FORMAT, GetSystemIdentifier());

    // Determine if we're in recovery mode and get appropriate WAL position
    am_cascading_walsender = RecoveryInProgress();
    if (am_cascading_walsender) {
        log_position = GetStandbyFlushRecPtr(&current_timeline);
    } else {
        log_position = GetFlushRecPtr(&current_timeline);
    }

    // Format WAL position as string
    snprintf(wal_location, sizeof(wal_location), "%X/%X", LSN_FORMAT_ARGS(log_position));

    // Get database name if connected to a specific database
    if (MyDatabaseId != InvalidOid) {
        MemoryContext saved_context = CurrentMemoryContext;
        StartTransactionCommand();
        MemoryContextSwitchTo(saved_context);
        database_name = get_database_name(MyDatabaseId);
        CommitTransactionCommand();
        MemoryContextSwitchTo(saved_context);
    }

    // Create result destination and tuple descriptor
    DestReceiver *destination = CreateDestReceiver(DestRemoteSimple);
    TupleDesc tuple_desc = CreateTemplateTupleDesc(4);

    // Define four columns: systemid, timeline, xlogpos, dbname
    TupleDescInitBuiltinEntry(tuple_desc, 1, "systemid", TEXTOID, -1, 0);
    TupleDescInitBuiltinEntry(tuple_desc, 2, "timeline", INT8OID, -1, 0);
    TupleDescInitBuiltinEntry(tuple_desc, 3, "xlogpos", TEXTOID, -1, 0);
    TupleDescInitBuiltinEntry(tuple_desc, 4, "dbname", TEXTOID, -1, 0);

    // Prepare tuple output
    TupOutputState *output_state = begin_tup_output_tupdesc(destination, tuple_desc, &TTSOpsVirtual);

    // Build result tuple with four values
    Datum values[4];
    bool nulls[4] = {false, false, false, false};

    values[0] = CStringGetTextDatum(system_id);        // System identifier
    values[1] = Int64GetDatum(current_timeline);       // Timeline ID
    values[2] = CStringGetTextDatum(wal_location);     // WAL position

    if (database_name) {
        values[3] = CStringGetTextDatum(database_name); // Database name
    } else {
        nulls[3] = true;                               // NULL if no database
    }

    // Send the result tuple and finish
    do_tup_output(output_state, values, nulls);
    end_tup_output(output_state);
}
```

Key simplifications made:
- Used more descriptive variable names (system_id, wal_location, etc.)
- Added clear comments explaining each logical step
- Consolidated the tuple building logic into a clearer sequence
- Removed detailed memory context comments while preserving the essential operations
- Focused on the main execution path and core functionality
- Maintained the essential algorithm for handling recovery vs normal mode