# libpqrcv_disconnect

## Location
[src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:880-903](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/libpqwalreceiver/libpqwalreceiver.c#L880-L903)

## Overview
Cleanly disconnects and frees resources associated with a WAL receiver connection to the primary server.

## Definition
```c
static void libpqrcv_disconnect(WalReceiverConn *conn)
```

## Detailed Description
This function performs a complete cleanup of a WAL receiver connection. It properly closes the libpq connection to the primary server, frees the receive buffer that was allocated for data reception, and deallocates the connection structure itself. This ensures that all resources associated with the replication connection are properly released.

The function is designed to be called when the WAL receiver is shutting down or when a connection error requires reconnection.

## Parameters / Member Variables
- `conn`: WAL receiver connection object to be disconnected and freed

## Dependencies
- Functions called/Symbols referenced:
  - [PQfinish](../P/PQfinish.md) (to close the libpq connection)
  - [PQfreemem](../P/PQfreemem.md) (to free the receive buffer allocated by libpq)
  - [pfree](../p/pfree.md) (to free the connection structure)
- Used by:
  - WAL receiver connection management functions
  - Connection cleanup routines during shutdown or error conditions

## Notes and Other Information
- This is a static function, only accessible within libpqwalreceiver.c
- Performs complete resource cleanup in the correct order
- Should only be called when the connection is no longer needed
- The function assumes the connection structure and its components were properly allocated
- Does not perform any error checking, assuming valid input
- Part of the cleanup sequence for WAL receiver operations