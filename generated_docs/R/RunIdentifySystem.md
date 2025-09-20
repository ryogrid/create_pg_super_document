# RunIdentifySystem

## Location
[src/bin/pg_basebackup/streamutil.c:480-560](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/streamutil.c#L480-L560)

## Overview
Executes the IDENTIFY_SYSTEM replication command through a PostgreSQL connection and retrieves system identification information including system identifier, timeline ID, start LSN position, and database name.

## Definition

```c
bool
RunIdentifySystem(PGconn *conn, char **sysid, TimeLineID *starttli,
				  XLogRecPtr *startpos, char **db_name)
```
## Detailed Description
This function sends the IDENTIFY_SYSTEM replication protocol command to a PostgreSQL server connection and parses the response to extract system identification information. The IDENTIFY_SYSTEM command is part of PostgreSQL's streaming replication protocol and returns essential information needed to establish replication connections or perform backup operations. The function validates the response format and extracts the requested information into caller-provided output parameters.

## Parameters / Member Variables
- `conn`: PostgreSQL connection handle (must not be NULL)
- `sysid`: Output parameter for system identifier string (optional, can be NULL)
- `starttli`: Output parameter for starting timeline ID (optional, can be NULL)  
- `startpos`: Output parameter for starting LSN position (optional, can be NULL)
- `db_name`: Output parameter for database name (optional, can be NULL, only available in PostgreSQL 9.4+)

## Dependencies
- Functions called/Symbols referenced:
  - [PQexec](../P/PQexec.md) - Execute SQL command
  - [PQresultStatus](../P/PQresultStatus.md) - Get result status
  - [PQntuples](../P/PQntuples.md) - Get number of result rows
  - [PQnfields](../P/PQnfields.md) - Get number of result fields
  - [PQgetvalue](../P/PQgetvalue.md) - Get field value from result
  - [PQserverVersion](../P/PQserverVersion.md) - Get server version
  - [PQgetisnull](../P/PQgetisnull.md) - Check if field is NULL
  - [PQclear](../P/PQclear.md) - Free result memory
  - pg_log_error - Log error messages
  - [pg_strdup](../p/pg_strdup.md) - Duplicate string
- Called from (representative examples):
  - [BaseBackup](../B/BaseBackup.md) (pg_basebackup.c:1826)
  - [StreamLog](../S/StreamLog.md) (pg_receivewal.c:531)
  - [main](../m/main.md) (pg_receivewal.c:851, pg_recvlogical.c:952)
  - [ReceiveXlogStream](ReceiveXlogStream.md) (receivelog.c:500)

## Notes and Other Information
- The function expects exactly 1 row with at least 3 fields in the result
- Database name extraction is only supported on PostgreSQL 9.4 and later versions
- LSN position is parsed from hexadecimal format (X/X) and converted to XLogRecPtr
- All output parameters are optional - callers can pass NULL for information they don't need
- Returns false on any error (connection issues, unexpected result format, parsing errors)
- Memory allocated for sysid and db_name strings should be freed by the caller using pg_free()