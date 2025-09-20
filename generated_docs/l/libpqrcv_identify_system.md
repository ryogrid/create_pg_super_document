# libpqrcv_identify_system

## Location
[src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:444-490](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/libpqwalreceiver/libpqwalreceiver.c#L444-L490)

## Overview
Checks that the primary server's system identifier matches the local system, and fetches the current timeline ID of the primary server during WAL receiver connection establishment.

## Definition

```c
static char *
libpqrcv_identify_system(WalReceiverConn *conn, TimeLineID *primary_tli)
```
## Detailed Description
This function establishes communication with a PostgreSQL primary server to verify system compatibility and retrieve essential replication information. It executes the  replication command, which returns the system identifier, timeline ID, and other metadata from the primary server. The function validates the response format and extracts the system identifier string and timeline ID, which are critical for ensuring that the standby server is connecting to the correct primary and can properly initialize WAL streaming replication.

The function performs strict validation of the response format, checking for the expected number of columns and rows. It supports both older (9.3 and earlier) and newer (9.4+) PostgreSQL versions that return different numbers of columns in the IDENTIFY_SYSTEM response.

## Parameters / Member Variables
- : Pointer to WalReceiverConn structure containing the established connection to the primary server
- : Output parameter that receives the current timeline ID from the primary server

## Dependencies
- Functions called/Symbols referenced:
  - [libpqrcv_PQexec](libpqrcv_PQexec.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - [PQnfields](../P/PQnfields.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [pg_strtoint32](../p/pg_strtoint32.md)
  - [pstrdup](../p/pstrdup.md)
  - [pchomp](../p/pchomp.md)
  - [PQclear](../P/PQclear.md)
- Called from (representative examples):
  - [WalReceiverConn](../W/WalReceiverConn.md) (referenced in connection establishment routines)

## Notes and Other Information
- This is a static function internal to the libpqwalreceiver module
- Returns a dynamically allocated string containing the primary system identifier that must be freed by the caller
- The function will raise an ERROR if the IDENTIFY_SYSTEM command fails or returns unexpected data
- Supports backward compatibility with PostgreSQL versions 9.3 and earlier (3 columns) vs 9.4+ (4+ columns)
- Located at src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:444-490