# GenerateRecoveryConfig

## Location
src/fe_utils/recovery_gen.c: 27 - 123

## Overview
GenerateRecoveryConfig creates recovery configuration content for PostgreSQL standby servers by building primary_conninfo and related settings from an existing database connection.

## Definition


## Detailed Description
This function generates recovery configuration content that can be written to postgresql.auto.conf or recovery.conf (for older versions). It extracts connection information from an active database connection and formats it for use by a standby server to connect to its primary. The function handles version-specific differences, such as the transition from standby_mode to standby.signal in PostgreSQL 12+.

The function builds a primary_conninfo string by iterating through the connection options of the provided PGconn, filtering out certain parameters that libpqwalreceiver will override (replication, dbname, fallback_application_name). It properly escapes and quotes the connection string for safe inclusion in configuration files.

## Parameters / Member Variables
- : Active database connection to extract connection parameters from
- : Optional replication slot name to be used for streaming replication
- : Optional database name to append to connection info (used by logical replication slot synchronization)

## Dependencies
- Functions called/Symbols referenced:
  - [PQserverVersion](../P/PQserverVersion.md)
  - [PQconninfo](../P/PQconninfo.md)
  - [PQconninfoFree](../P/PQconninfoFree.md)
  - createPQExpBuffer
  - initPQExpBuffer
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - appendPQExpBufferChar
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendConnStrVal](../a/appendConnStrVal.md)
  - termPQExpBuffer
  - PQExpBufferDataBroken
  - PQExpBufferBroken
  - [escape_quotes](../e/escape_quotes.md)
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - [BaseBackup](../B/BaseBackup.md) (pg_basebackup.c:1819)
  - [setup_recovery](../s/setup_recovery.md) (pg_createsubscriber.c:1205)
  - [main](../m/main.md) (pg_rewind.c:454, 531)

## Notes and Other Information
- Handles PostgreSQL version differences: adds 'standby_mode = on' for versions prior to 12
- Filters out connection parameters that libpqwalreceiver will override
- Uses escape_quotes() to safely escape the entire connection string for configuration file inclusion
- Returns a PQExpBuffer that must be freed by the caller
- Will call pg_fatal() on memory allocation failures
- The dbname parameter is specifically used for logical replication slot synchronization scenarios