# libpqrcv_send

## Location
[src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:994-1009](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/libpqwalreceiver/libpqwalreceiver.c#L994-L1009)

## Overview
libpqrcv_send is a static function that sends data to a PostgreSQL WAL (Write-Ahead Log) stream using the libpq connection API, providing error handling for stream communication failures.

## Definition

```c
static void
libpqrcv_send(WalReceiverConn *conn, const char *buffer, int nbytes)
```
## Detailed Description
This function serves as a low-level communication primitive for WAL replication streams in PostgreSQL. It wraps the libpq PQputCopyData and PQflush calls to send binary data through a streaming replication connection. The function implements robust error handling by checking the return values of both PQputCopyData (for queuing data) and PQflush (for ensuring data is actually transmitted), and raises an ERROR-level ereport if either operation fails. This ensures that WAL receiver processes can reliably detect and report communication problems with the primary server.

## Parameters / Member Variables
- `conn`: Pointer to WalReceiverConn structure containing the streaming connection to the primary server
- `buffer`: Pointer to the data buffer to be sent over the stream
- `nbytes`: Number of bytes to send from the buffer

## Dependencies
- Functions called/Symbols referenced:
  - [PQputCopyData](../P/PQputCopyData.md): libpq function to queue data for sending through a COPY stream
  - [PQflush](../P/PQflush.md): libpq function to flush any queued output data to the server
  - [pchomp](../p/pchomp.md): PostgreSQL utility function to remove trailing whitespace from error messages
  - ereport: PostgreSQL error reporting framework function
- Called from (representative examples):
  - Used internally within libpqwalreceiver.c for WAL streaming operations
  - Referenced by WalReceiverConn structure initialization

## Notes and Other Information
- This is a static function, meaning it's only accessible within the libpqwalreceiver.c compilation unit
- The function uses PostgreSQL's standard error reporting mechanism (ereport) rather than returning error codes
- Error handling follows the "fail fast" principle - any communication failure immediately terminates the operation
- The error message includes the actual libpq error details via PQerrorMessage for debugging purposes
- Location: src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:994-1009

## Simplified Source

```c
static void
libpqrcv_send(WalReceiverConn *conn, const char *buffer, int nbytes)
{
    // Send data and flush to ensure transmission
    if (PQputCopyData(conn->streamConn, buffer, nbytes) <= 0 ||
        PQflush(conn->streamConn))
        ereport(ERROR, (errmsg("could not send data to WAL stream: %s",
                               pchomp(PQerrorMessage(conn->streamConn)))));
}
```