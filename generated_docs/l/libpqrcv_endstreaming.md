# libpqrcv_endstreaming

## Location
[src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:655-731](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/libpqwalreceiver/libpqwalreceiver.c#L655-L731)

## Overview
Terminates an active WAL streaming session with the primary server, handling the proper protocol shutdown sequence and optionally retrieving the next timeline ID.

## Definition

```c
static void
libpqrcv_endstreaming(WalReceiverConn *conn, TimeLineID *next_tli)
```
## Detailed Description
This function implements the proper termination sequence for a WAL streaming connection in PostgreSQL replication. It sends a copy-end message to signal the end of streaming, then processes the server's response which may include information about the next timeline. The function handles multiple possible response scenarios: receiving timeline information in a result set, handling aborted copy operations, and ensuring proper protocol completion.

The function follows PostgreSQL's replication protocol strictly, verifying that all expected response messages are received in the correct order. It performs comprehensive error checking and cleanup, ensuring that the connection is left in a consistent state after streaming termination.

## Parameters / Member Variables
- `*conn`: Pointer to WalReceiverConn structure containing the active streaming connection
- `*next_tli`: Output parameter that receives the next timeline ID reported by the server, or 0 if not reported
## Dependencies
- Functions called/Symbols referenced:
  - [PQputCopyEnd](../P/PQputCopyEnd.md)
  - [PQflush](../P/PQflush.md)
  - [libpqrcv_PQgetResult](libpqrcv_PQgetResult.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - [PQnfields](../P/PQnfields.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [pg_strtoint32](../p/pg_strtoint32.md)
  - [PQendcopy](../P/PQendcopy.md)
  - [PQclear](../P/PQclear.md)
  - [pchomp](../p/pchomp.md)
- Called from (representative examples):
  - [WalReceiverConn](../W/WalReceiverConn.md) (referenced in streaming termination routines)

## Dependencies
- Functions called/Symbols referenced:
  - PGRES_TUPLES_OK
  - PGRES_COPY_OUT
  - PGRES_COMMAND_OK

## Notes and Other Information
- This is a static function internal to the libpqwalreceiver module
- The function handles multiple possible server response scenarios during streaming termination
- Performs rigorous protocol validation to ensure proper connection state after termination
- The next timeline ID is optional information that may not be provided by all server versions
- Handles both normal termination and error scenarios like mid-stream copy abortion
- Verifies that no unexpected results remain after command completion
- Located at src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:655-731

## Simplified Source

```c
static void
libpqrcv_endstreaming(WalReceiverConn *conn, TimeLineID *next_tli)
{
    PGresult *res;

    // Send end-of-streaming message
    if (PQputCopyEnd(conn->streamConn, NULL) <= 0 || PQflush(conn->streamConn))
        ereport(ERROR, (errmsg("could not send end-of-streaming message to primary: %s",
                               pchomp(PQerrorMessage(conn->streamConn)))));

    *next_tli = 0;

    // Get server response after ending copy mode
    res = libpqrcv_PQgetResult(conn->streamConn);

    if (PQresultStatus(res) == PGRES_TUPLES_OK) {
        // Server sent timeline information
        if (PQnfields(res) < 2 || PQntuples(res) != 1)
            ereport(ERROR, (errmsg("unexpected result set after end-of-streaming")));

        *next_tli = pg_strtoint32(PQgetvalue(res, 0, 0));
        PQclear(res);

        // Expect CommandComplete to follow
        res = libpqrcv_PQgetResult(conn->streamConn);
    }
    else if (PQresultStatus(res) == PGRES_COPY_OUT) {
        // Handle aborted copy case
        PQclear(res);

        if (PQendcopy(conn->streamConn))
            ereport(ERROR, (errmsg("error while shutting down streaming COPY: %s",
                                   pchomp(PQerrorMessage(conn->streamConn)))));

        // Get CommandComplete
        res = libpqrcv_PQgetResult(conn->streamConn);
    }

    // Verify we got CommandComplete
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        ereport(ERROR, (errmsg("error reading result of streaming command: %s",
                               pchomp(PQerrorMessage(conn->streamConn)))));
    PQclear(res);

    // Ensure no additional unexpected results
    res = libpqrcv_PQgetResult(conn->streamConn);
    if (res != NULL)
        ereport(ERROR, (errmsg("unexpected result after CommandComplete: %s",
                               pchomp(PQerrorMessage(conn->streamConn)))));
}
```