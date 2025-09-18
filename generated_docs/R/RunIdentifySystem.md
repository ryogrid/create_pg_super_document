# RunIdentifySystem

## Location
src/bin/pg_basebackup/streamutil.c: 480 - 560

## Overview
Executes the IDENTIFY_SYSTEM replication command through a PostgreSQL connection and retrieves system identification information including system identifier, timeline ID, start LSN position, and database name.

## Definition


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
  - PQexec - Execute SQL command
  - PQresultStatus - Get result status
  - PQntuples - Get number of result rows
  - PQnfields - Get number of result fields
  - PQgetvalue - Get field value from result
  - PQserverVersion - Get server version
  - PQgetisnull - Check if field is NULL
  - PQclear - Free result memory
  - pg_log_error - Log error messages
  - pg_strdup - Duplicate string
- Called from (representative examples):
  - BaseBackup (pg_basebackup.c:1826)
  - StreamLog (pg_receivewal.c:531)
  - main (pg_receivewal.c:851, pg_recvlogical.c:952)
  - ReceiveXlogStream (receivelog.c:500)

## Notes and Other Information
- The function expects exactly 1 row with at least 3 fields in the result
- Database name extraction is only supported on PostgreSQL 9.4 and later versions
- LSN position is parsed from hexadecimal format (X/X) and converted to XLogRecPtr
- All output parameters are optional - callers can pass NULL for information they don't need
- Returns false on any error (connection issues, unexpected result format, parsing errors)
- Memory allocated for sysid and db_name strings should be freed by the caller using pg_free()