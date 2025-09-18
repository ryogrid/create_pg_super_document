# wait_until_connected

## Location
src/bin/psql/command.c: 3850 - 3911

## Overview
Processes the asynchronous connection sequence initiated by PQconnectStartParams(), polling the connection until completion while handling user cancellation requests.

## Definition


## Detailed Description
The  function implements a polling loop to complete PostgreSQL database connections initiated in asynchronous mode. It handles the complex state machine of libpq's non-blocking connection establishment, managing socket polling for both read and write operations as needed by the connection protocol.

Key behaviors include:
- **Asynchronous connection handling**: Uses PQconnectPoll() to advance the connection state machine without blocking indefinitely
- **Cancellation support**: Checks for user cancellation (SIGINT) on each iteration to allow graceful termination
- **Timeout-based polling**: Uses a 1-second timeout on socket operations to periodically check for cancellation, solving the race condition between signal handling and socket polling
- **Socket state management**: Adapts to changing socket file descriptors and poll directions (read vs write) as required by the connection protocol
- **Error resilience**: Handles various connection states and socket errors without reporting them directly (error reporting is delegated to the caller)

## Parameters / Member Variables
- : PGconn pointer representing the database connection being established

## Dependencies
- Functions called/Symbols referenced:
  - : Retrieves the current socket file descriptor for the connection
  - : Gets current time for timeout calculation
  - : Polls socket for I/O readiness with timeout
  - : Advances the connection state machine
  - : Marks unreachable code paths (for PGRES_POLLING_ACTIVE case)
- Called from (representative examples):
  - : Main connection establishment function

## Notes and Other Information
- The function implements a careful balance between responsiveness to user cancellation and connection protocol requirements
- Uses a 1-second timeout as a simple solution to the SIGINT race condition, avoiding more complex signal handling mechanisms like the "self-pipe trick"
- The socket file descriptor may change between PQconnectPoll() calls, so it's retrieved fresh on each iteration
- PGRES_POLLING_ACTIVE state is considered unreachable in this context and triggers an assertion failure
- Error reporting is intentionally deferred to the caller, which checks the final connection status