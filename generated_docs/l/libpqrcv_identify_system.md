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
- `*conn`: Pointer to WalReceiverConn structure containing the established connection to the primary server
- `*primary_tli`: Output parameter that receives the current timeline ID from the primary server
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

## Simplified Source

```c
static char *libpqrcv_identify_system(WalReceiverConn *conn, TimeLineID *primary_tli)
{
    PGresult *res;
    char *primary_sysid;

    // Execute IDENTIFY_SYSTEM command on primary server
    res = libpqrcv_PQexec(conn->streamConn, "IDENTIFY_SYSTEM");
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        PQclear(res);
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                       errmsg("could not receive database system identifier and timeline ID from "
                              "the primary server: %s",
                              pchomp(PQerrorMessage(conn->streamConn)))));
    }

    // Validate response format (3 columns in 9.3-, 4+ columns in 9.4+)
    if (PQnfields(res) < 3 || PQntuples(res) != 1)
    {
        int ntuples = PQntuples(res);
        int nfields = PQnfields(res);

        PQclear(res);
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                       errmsg("invalid response from primary server"),
                       errdetail("Could not identify system: got %d rows and %d fields, "
                                "expected %d rows and %d or more fields.",
                                ntuples, nfields, 1, 3)));
    }

    // Extract system identifier and timeline ID
    primary_sysid = pstrdup(PQgetvalue(res, 0, 0));
    *primary_tli = pg_strtoint32(PQgetvalue(res, 0, 1));
    PQclear(res);

    return primary_sysid;
}
```