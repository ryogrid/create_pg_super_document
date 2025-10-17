# GetSlotInformation

## Location
[src/bin/pg_basebackup/streamutil.c:561-654](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/streamutil.c#L561-L654)

## Overview
Executes the READ_REPLICATION_SLOT replication command to retrieve information about a specified physical replication slot, including its restart LSN position and current timeline ID.

## Definition

```c
bool
GetSlotInformation(PGconn *conn, const char *slot_name,
				   XLogRecPtr *restart_lsn, TimeLineID *restart_tli)
```
## Detailed Description
This function sends the READ_REPLICATION_SLOT replication protocol command to query information about a named physical replication slot. It validates that the slot exists and is of the correct type (physical), then extracts the restart LSN position and timeline ID from the response. The function is designed to work specifically with physical replication slots and will return an error if the slot is logical or doesn't exist.

## Parameters / Member Variables
- `conn`: PostgreSQL connection handle for the replication command
- `slot_name`: Name of the replication slot to query
- `restart_lsn`: Output parameter for the slot's restart LSN position (optional, can be NULL)
- `restart_tli`: Output parameter for the slot's current timeline ID (optional, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md) - Create dynamic string buffer
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md) - [Append](../A/Append.md) formatted data to buffer
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md) - Free buffer memory
  - [PQexec](../P/PQexec.md) - Execute SQL command
  - [PQresultStatus](../P/PQresultStatus.md) - Get result status
  - [PQntuples](../P/PQntuples.md) - Get number of result rows
  - [PQnfields](../P/PQnfields.md) - Get number of result fields
  - [PQgetisnull](../P/PQgetisnull.md) - Check if field is NULL
  - [PQgetvalue](../P/PQgetvalue.md) - Get field value from result
  - [PQclear](../P/PQclear.md) - Free result memory
  - pg_log_error - Log error messages
  - strcmp - [String](../S/String.md) comparison
  - sscanf - Parse formatted string
  - atol - Convert string to long integer
- Called from (representative examples):
  - [StreamLog](../S/StreamLog.md) (pg_receivewal.c:547)

## Notes and Other Information
- The function expects exactly 1 row with 3 fields in the result (slot_type, restart_lsn, restart_tli)
- Only supports physical replication slots - returns error for logical slots
- Returns InvalidXLogRecPtr for restart_lsn and 0 for restart_tli if the slot information is not available
- LSN is parsed from hexadecimal format (X/X) and converted to XLogRecPtr
- If the slot doesn't exist, READ_REPLICATION_SLOT returns a tuple with NULL values
- Both output parameters are optional - callers can pass NULL for information they don't need
- Returns false on any error (connection issues, slot doesn't exist, wrong slot type, parsing errors)

## Simplified Source

```c
bool GetSlotInformation(PGconn *conn, const char *slot_name,
                       XLogRecPtr *restart_lsn, TimeLineID *restart_tli) {
    PGresult *res;
    PQExpBuffer query;
    XLogRecPtr lsn_loc = InvalidXLogRecPtr;
    TimeLineID tli_loc = 0;

    // Initialize output parameters
    if (restart_lsn) *restart_lsn = lsn_loc;
    if (restart_tli) *restart_tli = tli_loc;

    // Build and execute READ_REPLICATION_SLOT command
    query = createPQExpBuffer();
    appendPQExpBuffer(query, "READ_REPLICATION_SLOT %s", slot_name);
    res = PQexec(conn, query->data);
    destroyPQExpBuffer(query);

    // Check command execution status
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log_error("could not send replication command \"READ_REPLICATION_SLOT\": %s",
                     PQerrorMessage(conn));
        PQclear(res);
        return false;
    }

    // Validate result format (expect 1 row, 3 fields)
    if (PQntuples(res) != 1 || PQnfields(res) != 3) {
        pg_log_error("unexpected result format from READ_REPLICATION_SLOT");
        PQclear(res);
        return false;
    }

    // Check if slot exists (slot_type field is NULL if slot doesn't exist)
    if (PQgetisnull(res, 0, 0)) {
        pg_log_error("replication slot \"%s\" does not exist", slot_name);
        PQclear(res);
        return false;
    }

    // Verify this is a physical slot
    if (strcmp(PQgetvalue(res, 0, 0), "physical") != 0) {
        pg_log_error("expected physical replication slot, got type \"%s\"",
                     PQgetvalue(res, 0, 0));
        PQclear(res);
        return false;
    }

    // Parse restart LSN if available
    if (!PQgetisnull(res, 0, 1)) {
        uint32 hi, lo;
        if (sscanf(PQgetvalue(res, 0, 1), "%X/%X", &hi, &lo) != 2) {
            pg_log_error("could not parse restart_lsn for slot \"%s\"", slot_name);
            PQclear(res);
            return false;
        }
        lsn_loc = ((uint64) hi) << 32 | lo;
    }

    // Parse timeline ID if available
    if (!PQgetisnull(res, 0, 2))
        tli_loc = (TimeLineID) atol(PQgetvalue(res, 0, 2));

    PQclear(res);

    // Set output parameters
    if (restart_lsn) *restart_lsn = lsn_loc;
    if (restart_tli) *restart_tli = tli_loc;

    return true;
}
```