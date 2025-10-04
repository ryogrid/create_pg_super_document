# PQrequestCancel

## Location
[src/interfaces/libpq/fe-cancel.c:662-703](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-cancel.c#L662-L703)

## Overview
A convenience wrapper function that requests query cancellation on an active PostgreSQL connection using the old, non-thread-safe cancellation protocol.

## Definition

```c
int
PQrequestCancel(PGconn *conn)
```
## Detailed Description
PQrequestCancel provides a simplified interface for canceling queries on an existing PostgreSQL connection. It internally creates a PGcancel object from the connection, sends the cancel request using PQcancel, and then cleans up the cancel object. This function is marked as old and not thread-safe because it modifies the connection's error message buffer, which could interfere with other concurrent operations on the same connection object. The function performs validation to ensure the connection is open before attempting cancellation.

## Parameters / Member Variables
- `*conn`: Pointer to an active PGconn connection object from which to extract cancellation information
## Dependencies
- Functions called/Symbols referenced:
  - [PQgetCancel](PQgetCancel.md) (creates PGcancel object from connection)
  - [PQcancel](PQcancel.md) (sends the actual cancel request)
  - [PQfreeCancel](PQfreeCancel.md) (cleans up the cancel object)
  - [strlcpy](../s/strlcpy.md) (safe string copying for error messages)
  - strlen (string length calculation)
- Called from (representative examples):
  - [test_cancel](../t/test_cancel.md) (src/test/modules/libpq_pipeline/libpq_pipeline.c:281)
  - PQsetdb (referenced in src/interfaces/libpq/libpq-fe.h:380)

## Notes and Other Information
- This is the old, legacy API for query cancellation - newer applications should use PQgetCancel/PQcancel/PQfreeCancel directly
- Not thread-safe due to modification of conn->errorMessage without synchronization
- Error messages are stored directly in the connection object's error buffer
- Automatically handles the creation and cleanup of the PGcancel object
- Validates that the connection socket is open before attempting cancellation
- Returns true on successful dispatch, false on failure
- Error messages may be truncated if they exceed the connection's error buffer size
- Location: src/interfaces/libpq/fe-cancel.c:662-703

## Simplified Source

```c
int PQrequestCancel(PGconn *conn) {
    int r;
    PGcancel *cancel;

    // Validate connection is open
    if (!conn)
        return false;

    if (conn->sock == PGINVALID_SOCKET) {
        strlcpy(conn->errorMessage.data,
                "PQrequestCancel() -- connection is not open\n",
                conn->errorMessage.maxlen);
        conn->errorMessage.len = strlen(conn->errorMessage.data);
        conn->errorReported = 0;
        return false;
    }

    // Create cancel object from connection
    cancel = PQgetCancel(conn);
    if (cancel) {
        // Send cancel request using cancel object
        r = PQcancel(cancel, conn->errorMessage.data, conn->errorMessage.maxlen);
        PQfreeCancel(cancel);
    } else {
        strlcpy(conn->errorMessage.data, "out of memory", conn->errorMessage.maxlen);
        r = false;
    }

    // Update error message length on failure
    if (!r) {
        conn->errorMessage.len = strlen(conn->errorMessage.data);
        conn->errorReported = 0;
    }

    return r;
}
```