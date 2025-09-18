# libpqrcv_identify_system

## Location
src/backend/replication/libpqwalreceiver/libpqwalreceiver.c: 444 - 490

## Overview
Checks that the primary server's system identifier matches the local system, and fetches the current timeline ID of the primary server during WAL receiver connection establishment.

## Definition


## Detailed Description
This function establishes communication with a PostgreSQL primary server to verify system compatibility and retrieve essential replication information. It executes the  replication command, which returns the system identifier, timeline ID, and other metadata from the primary server. The function validates the response format and extracts the system identifier string and timeline ID, which are critical for ensuring that the standby server is connecting to the correct primary and can properly initialize WAL streaming replication.

The function performs strict validation of the response format, checking for the expected number of columns and rows. It supports both older (9.3 and earlier) and newer (9.4+) PostgreSQL versions that return different numbers of columns in the IDENTIFY_SYSTEM response.

## Parameters / Member Variables
- : Pointer to WalReceiverConn structure containing the established connection to the primary server
- : Output parameter that receives the current timeline ID from the primary server

## Dependencies
- Functions called/Symbols referenced:
  - libpqrcv_PQexec
  - PQresultStatus
  - PQnfields
  - PQntuples
  - PQgetvalue
  - pg_strtoint32
  - pstrdup
  - pchomp
  - PQclear
- Called from (representative examples):
  - WalReceiverConn (referenced in connection establishment routines)

## Notes and Other Information
- This is a static function internal to the libpqwalreceiver module
- Returns a dynamically allocated string containing the primary system identifier that must be freed by the caller
- The function will raise an ERROR if the IDENTIFY_SYSTEM command fails or returns unexpected data
- Supports backward compatibility with PostgreSQL versions 9.3 and earlier (3 columns) vs 9.4+ (4+ columns)
- Located at src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:444-490