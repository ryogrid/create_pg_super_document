# handleSyncLoss

## Location
[src/interfaces/libpq/fe-protocol3.c:483-502](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-protocol3.c#L483-L502)

## Overview
handleSyncLoss is an error recovery function that handles loss of message-boundary synchronization with the PostgreSQL server by terminating the connection and setting appropriate error states.

## Definition

```c
static void
handleSyncLoss(PGconn *conn, char id, int msgLength)
```
## Detailed Description
This function is called when the client detects that it has lost synchronization with the server's message protocol. This typically occurs when message boundaries become corrupted or when invalid message types or lengths are encountered. Since there is no reliable way to recover from synchronization loss in a streaming protocol, the function takes the drastic but necessary step of abandoning the connection entirely. It logs the problematic message details, creates an error result, and marks the connection as bad to prevent further use.

The function serves as a critical safety mechanism in the PostgreSQL protocol implementation, ensuring that corrupted or malformed protocol streams don't cause undefined behavior or security issues.

## Parameters / Member Variables
- : Pointer to the PGconn structure representing the database connection that has lost synchronization
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): The message type character that caused the synchronization issue
- : The problematic message length that was received

## Dependencies
- Functions called/Symbols referenced:
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md) (appends error message to connection error buffer)
  - [pqSaveErrorResult](../p/pqSaveErrorResult.md) (saves the current error state as a result)
  - PGASYNC_READY (sets connection to ready state to exit wait loops)
  - [pqDropConnection](../p/pqDropConnection.md) (closes the network connection)
  - CONNECTION_BAD (marks connection status as unusable)
- Called from (representative examples):
  - [pqParseInput3](../p/pqParseInput3.md) (multiple locations: lines 94, 99, 127)
  - [getCopyDataMessage](../g/getCopyDataMessage.md) (lines 1662, 1681)
  - [pqFunctionCall3](../p/pqFunctionCall3.md) (lines 2090, 2095, 2119)
  - VALID_LONG_MESSAGE_TYPE (line 46)

## Notes and Other Information
- This is a static function, only accessible within fe-protocol3.c
- Represents an unrecoverable error condition - once called, the connection cannot be reused
- The function is designed to fail fast and cleanly rather than attempt potentially unsafe recovery
- Critical for maintaining protocol integrity and preventing potential security vulnerabilities from malformed messages
- The error message includes both the problematic message type and length for debugging purposes
- Forces the connection into READY state specifically to break out of any pending PQgetResult wait loops

## Simplified Source

```c
static void
handleSyncLoss(PGconn *conn, char message_id, int message_length)
{
    // Log the synchronization error with problematic message details
    libpq_append_conn_error(conn,
        "lost synchronization with server: got message type \"%c\", length %d",
        message_id, message_length);

    // Create and save an error result
    pqSaveErrorResult(conn);

    // Exit any PQgetResult wait loops
    conn->asyncStatus = PGASYNC_READY;

    // Close the network connection and mark it as unusable
    pqDropConnection(conn, true);
    conn->status = CONNECTION_BAD;
}
```