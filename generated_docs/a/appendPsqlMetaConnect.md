# appendPsqlMetaConnect

## Location
src/fe_utils/string_utils.c: 743 - 818

## Overview
Appends a psql meta-command that connects to the given database using the current connection's user, host, and port parameters.

## Definition


## Detailed Description
This function generates a psql meta-command to connect to a specified database. It analyzes the database name to determine the appropriate connection syntax:

1. For simple ASCII names (containing only letters, digits, underscores, and periods), it generates a simple  command
2. For complex names (containing special characters), it uses the more robust  format with proper connection string encoding

The function ensures proper handling of special characters by:
- Checking for invalid characters like newlines/carriage returns (causes program exit)
- Using SQL_ASCII encoding for complex database names
- Applying proper identifier quoting through 

## Parameters / Member Variables
- : PQExpBuffer to append the meta-command to
- : The target database name to connect to

## Dependencies
- Functions called/Symbols referenced:
  - initPQExpBuffer
  - [appendPQExpBufferStr](appendPQExpBufferStr.md)
  - [appendConnStrVal](appendConnStrVal.md)  
  - [fmtIdEnc](../f/fmtIdEnc.md)
  - termPQExpBuffer
  - appendPQExpBufferChar
  - PG_SQL_ASCII
  - EXIT_FAILURE
  - [PQExpBufferData](../P/PQExpBufferData.md)

- Called from (representative examples):
  - [_reconnectToDB](../r/_reconnectToDB.md) (src/bin/pg_dump/pg_backup_archiver.c:3382)
  - [old_9_6_invalidate_hash_indexes](../o/old_9_6_invalidate_hash_indexes.md) (src/bin/pg_upgrade/version.c:85)
  - [report_extension_updates](../r/report_extension_updates.md) (src/bin/pg_upgrade/version.c:183)

## Notes and Other Information
- Located in src/fe_utils/string_utils.c:743-818
- Exits with EXIT_FAILURE if database name contains newline or carriage return characters
- Forces SQL_ASCII encoding for complex database names to ensure proper forwarding to server
- Uses different quoting strategies based on database name complexity to maintain compatibility with different PostgreSQL versions
- Part of the frontend utilities library for database connection management