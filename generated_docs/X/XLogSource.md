# XLogSource

## Location
[src/backend/access/transam/xlogrecovery.c:215-303](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L215-L303)

## Overview
XLogSource is an enumeration that defines the various sources from which Write-Ahead Log (WAL) data can be read during PostgreSQL recovery operations.

## Definition

```c
typedef enum
{
	XLOG_FROM_ANY = 0,			/* request to read WAL from any source */
	XLOG_FROM_ARCHIVE,			/* restored using restore_command */
	XLOG_FROM_PG_WAL,			/* existing file in pg_wal */
	XLOG_FROM_STREAM,			/* streamed from primary */
} XLogSource;
```
## Detailed Description
XLogSource is a critical enumeration used in PostgreSQL's WAL recovery system to track and control where WAL data is being read from during recovery operations. This enumeration allows the recovery system to maintain awareness of the current data source and implement appropriate fallback strategies when one source becomes unavailable.

The enumeration is used throughout the recovery process to:
- Track the current source of WAL data being processed
- Implement source failover logic during recovery
- Provide debugging information about WAL data sources
- Control recovery behavior based on the available sources

The recovery system maintains several XLogSource variables to track different aspects of WAL reading:
- : indicates where the currently open file came from
- : tracks which source is currently being used
- : tracks where WAL was last successfully obtained

## Parameters / Member Variables
- : Generic value (0) used to request reading WAL from any available source, typically used as an initial or reset state
- : WAL data restored from archive using the restore_command configuration
- : WAL data read from existing files in the pg_wal directory
- : WAL data streamed directly from the primary server during streaming replication

## Dependencies
- Functions that use XLogSource:
  - [WaitForWALToBecomeAvailable](../W/WaitForWALToBecomeAvailable.md)
  - [XLogFileRead](XLogFileRead.md)
  - [XLogFileReadAnyTLI](XLogFileReadAnyTLI.md)
- Related types:
  - XLogSegNo
- Associated constants and arrays:
  - xlogSourceNames[] (human-readable names for debugging)

## Notes and Other Information
- The enumeration includes a corresponding xlogSourceNames array with human-readable names ('any', 'archive', 'pg_wal', 'stream') for debugging output
- The recovery system uses multiple XLogSource variables to track different states: current source, last successful source, and source of currently open file
- Source switching logic is implemented in WaitForWALToBecomeAvailable, which handles fallback between different WAL sources
- The XLOG_FROM_ANY value (0) serves as both an initial state and a request to read from any available source
- This enumeration is essential for implementing robust WAL recovery with multiple source failover capabilities