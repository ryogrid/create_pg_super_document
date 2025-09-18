# _reconnectToDB

## Location
[src/bin/pg_dump/pg_backup_archiver.c:3365-3415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L3365-L3415)

## Overview
A static function that handles database reconnection during PostgreSQL restore operations, either by establishing an actual connection or outputting psql meta-commands to change databases.

## Definition
```c
static void _reconnectToDB(ArchiveHandle *AH, const char *dbname)
```

## Detailed Description
This function manages the process of switching to a different database during restore operations. It operates in two distinct modes depending on the restore context:

1. **Connected mode**: When restoring directly to a database, it calls ReconnectToServer() to establish a new database connection
2. **Script mode**: When generating a script file, it outputs psql meta-commands including \connect to change databases

In script mode, the function temporarily exits restricted mode to safely execute the \connect command, then re-enters restricted mode. This security measure prevents potential meta-command injection while allowing necessary database switching.

After reconnection, the function resets session state tracking variables (current user, schema, table access method, and tablespace) and re-establishes the fixed output state to ensure consistent restore behavior in the new database context.

## Parameters / Member Variables
- `AH`: Pointer to ArchiveHandle structure containing restore context and connection information
- `dbname`: Name of the target database to connect to

## Dependencies
- Functions called/Symbols referenced:
  - [RestoringToDB](../R/RestoringToDB.md) (connection mode check)
  - [ReconnectToServer](../R/ReconnectToServer.md) (actual database reconnection)
  - [PQExpBufferData](../P/PQExpBufferData.md) (buffer data structure)
  - [RestoreOptions](../R/RestoreOptions.md) (restore configuration structure)
  - [ahprintf](../a/ahprintf.md) (archive output function)
  - initPQExpBuffer (buffer initialization)
  - [appendPsqlMetaConnect](../a/appendPsqlMetaConnect.md) (psql connection command generation)
  - termPQExpBuffer (buffer cleanup)
  - free (memory deallocation)
  - [_doSetFixedOutputState](../d/_doSetFixedOutputState.md) (session state re-establishment)
- Called from (representative examples):
  - [restore_toc_entry](restore_toc_entry.md)

## Notes and Other Information
- Located in src/bin/pg_dump/pg_backup_archiver.c:3365-3415
- Implements security measures to prevent meta-command injection attacks in script mode
- Resets session state tracking to ensure accuracy after database switch
- Essential for multi-database restore operations where objects exist in different databases
- Uses restrict/unrestrict mechanism to safely handle psql meta-commands
- Automatically re-establishes consistent session parameters after reconnection