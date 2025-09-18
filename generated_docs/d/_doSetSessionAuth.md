# _doSetSessionAuth

## Location
[src/bin/pg_dump/pg_backup_archiver.c:3322-3364](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L3322-L3364)

## Overview
A static function that executes a SET SESSION AUTHORIZATION command to change the session user during PostgreSQL database restoration operations.

## Definition
```c
static void _doSetSessionAuth(ArchiveHandle *AH, const char *user)
```

## Detailed Description
This function constructs and executes a SET SESSION AUTHORIZATION SQL command to change the current session's authorization identifier. It handles both cases where a specific user is provided and where the default authorization should be restored. The function properly formats the user name as a SQL string literal and executes the command either directly against a live database connection or outputs it to a script file.

The function supports two operational modes:
- Connected mode: Executes the command directly via PQexec() and handles any errors
- Script mode: Outputs the command to the archive for later execution

If the user parameter is NULL or empty, the command uses DEFAULT to restore the original session authorization.

## Parameters / Member Variables
- `AH`: Pointer to ArchiveHandle structure containing the restore context
- `user`: Target username for session authorization, or NULL/empty string for DEFAULT

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer (buffer creation)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md) (string buffer operations)
  - appendStringLiteralAHX (safe string literal formatting)
  - appendPQExpBufferChar (character buffer operations)
  - [RestoringToDB](../R/RestoringToDB.md) (connection mode check)
  - [PQexec](../P/PQexec.md) (PostgreSQL command execution)
  - PGRES_COMMAND_OK (result status constant)
  - [PQerrorMessage](../P/PQerrorMessage.md) (error message retrieval)
  - [PQclear](../P/PQclear.md) (result cleanup)
  - [ahprintf](../a/ahprintf.md) (archive output)
  - destroyPQExpBuffer (buffer cleanup)
- Called from (representative examples):
  - [_becomeUser](../b/_becomeUser.md)

## Notes and Other Information
- Located in src/bin/pg_dump/pg_backup_archiver.c:3322-3364
- Uses proper SQL string literal formatting to prevent SQL injection
- Provides specific error handling with pg_fatal() rather than warn_or_exit_horribly()
- The caller is responsible for updating any necessary state after the authorization change
- Essential for privilege separation during restore operations where different objects may need to be created under different user contexts