# libpqrcv_startstreaming

## Location
[src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:551-654](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/libpqwalreceiver/libpqwalreceiver.c#L551-L654)

## Overview
Initiates WAL (Write-Ahead Log) streaming from a PostgreSQL primary server using specified streaming options, supporting both physical and logical replication modes.

## Definition


## Detailed Description
This function constructs and executes a START_REPLICATION command to begin streaming WAL data from a PostgreSQL primary server. It supports both physical and logical replication modes, with different parameter handling for each. For logical replication, it processes complex options like publication names, protocol version, streaming mode, two-phase commit support, and binary format. For physical replication, it specifies the timeline ID. 

The function performs version-specific feature checking, ensuring that advanced features like two-phase commit and origin tracking are only used with compatible server versions. It handles proper string escaping and memory management throughout the command construction process.

## Parameters / Member Variables
- : Pointer to WalReceiverConn structure containing the connection to the primary server
- : Pointer to WalRcvStreamOptions structure containing all streaming configuration parameters including start point, slot name, and replication mode-specific settings

## Dependencies
- Functions called/Symbols referenced:
  - initStringInfo
  - appendStringInfoString
  - appendStringInfo
  - appendStringInfoChar
  - [stringlist_to_identifierstr](../s/stringlist_to_identifierstr.md)
  - [PQescapeLiteral](../P/PQescapeLiteral.md)
  - [PQfreemem](../P/PQfreemem.md)
  - [PQserverVersion](../P/PQserverVersion.md)
  - [libpqrcv_PQexec](libpqrcv_PQexec.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - [pchomp](../p/pchomp.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [WalReceiverConn](../W/WalReceiverConn.md) (referenced in streaming setup routines)

## Dependencies
- Functions called/Symbols referenced:
  - LSN_FORMAT_ARGS (macro for formatting LSN)
  - PGRES_COMMAND_OK
  - PGRES_COPY_BOTH

## Notes and Other Information
- This is a static function internal to the libpqwalreceiver module
- Returns true if successfully switched to copy-both mode (streaming active), false if command succeeded but no WAL available
- Throws ERROR on failure conditions including protocol violations and memory allocation errors
- Supports server version compatibility checks for features like two-phase commit (15.0+), origin tracking (16.0+), and binary format (14.0+)
- Handles both slot-based and slotless replication scenarios
- Properly escapes publication names to prevent SQL injection in logical replication
- Located at src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:551-654