# IdentifySystem

## Location
src/backend/replication/walsender.c: 411 - 493

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
  - GetSystemIdentifier
  - RecoveryInProgress
  - GetStandbyFlushRecPtr
  - GetFlushRecPtr
  - StartTransactionCommand
  - get_database_name
  - CommitTransactionCommand
  - CreateDestReceiver
  - CreateTemplateTupleDesc
  - TupleDescInitBuiltinEntry
  - begin_tup_output_tupdesc
  - Int64GetDatum
  - do_tup_output
  - end_tup_output
  - DestRemoteSimple
  - MAXFNAMELEN
  - UINT64_FORMAT

- Called from:
  - exec_replication_command (when processing IDENTIFY_SYSTEM command)

## Notes and Other Information
- This is a static function only accessible within walsender.c
- The function handles both regular and cascading WAL sender scenarios differently for WAL position retrieval
- Uses temporary transaction context for database name lookup since syscache access requires transaction environment
- Returns a result set with exactly four columns: systemid (text), timeline (int8), xlogpos (text), dbname (text or NULL)
- The WAL position is formatted as a string in PostgreSQL's standard LSN format (XXX/XXX)
- Database name is set to NULL if not connected to a specific database (MyDatabaseId == InvalidOid)
- This command is fundamental to PostgreSQL's streaming replication protocol and is typically the first command executed by replication clients
- The system identifier uniquely identifies a PostgreSQL cluster and is used to prevent replication between incompatible systems