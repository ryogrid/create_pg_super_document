# wait_until_connected

## Location
[src/bin/psql/command.c:3850-3911](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L3850-L3911)

## Overview
Processes the asynchronous connection sequence initiated by PQconnectStartParams(), polling the connection until completion while handling user cancellation requests.

## Definition

```c
static void
wait_until_connected(PGconn *conn)
```
## Detailed Description
The  function implements a polling loop to complete PostgreSQL database connections initiated in asynchronous mode. It handles the complex state machine of libpq's non-blocking connection establishment, managing socket polling for both read and write operations as needed by the connection protocol.

Key behaviors include:
- **Asynchronous connection handling**: Uses PQconnectPoll() to advance the connection state machine without blocking indefinitely
- **Cancellation support**: Checks for user cancellation (SIGINT) on each iteration to allow graceful termination
- **Timeout-based polling**: Uses a 1-second timeout on socket operations to periodically check for cancellation, solving the race condition between signal handling and socket polling
- **Socket state management**: Adapts to changing socket file descriptors and poll directions (read vs write) as required by the connection protocol
- **Error resilience**: Handles various connection states and socket errors without reporting them directly (error reporting is delegated to the caller)

## Parameters / Member Variables
- `*conn`: PGconn pointer representing the database connection being established
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

## Simplified Source

```c
static void wait_until_connected(PGconn *conn)
{
    bool forRead = false;

    while (true) {
        // Check for user cancellation on each iteration
        if (cancel_pressed)
            break;

        // Get current socket (may change between polls)
        int sock = PQsocket(conn);
        if (sock == -1)
            break;

        // Poll socket with 1-second timeout to handle cancellation race condition
        pg_usec_time_t end_time = PQgetCurrentTimeUSec() + 1000000; // 1 second
        int rc = PQsocketPoll(sock, forRead, !forRead, end_time);
        if (rc == -1)
            return;

        // Advance connection state machine
        switch (PQconnectPoll(conn)) {
            case PGRES_POLLING_OK:
            case PGRES_POLLING_FAILED:
                return; // Connection complete (success or failure)

            case PGRES_POLLING_READING:
                forRead = true;
                continue; // Need to wait for read

            case PGRES_POLLING_WRITING:
                forRead = false;
                continue; // Need to wait for write

            case PGRES_POLLING_ACTIVE:
                pg_unreachable(); // Should not happen
        }
    }
}
```

**Simplified Logic:**
1. **Loop until connection completes**: Continue polling until success, failure, or cancellation
2. **Check cancellation**: Test for SIGINT on each iteration for responsiveness
3. **Get socket**: Retrieve current socket FD (may change during connection process)
4. **Poll with timeout**: Wait up to 1 second for socket I/O readiness to avoid blocking indefinitely
5. **Advance state machine**: Call PQconnectPoll() to process next connection step
6. **Handle states**: Set read/write direction based on what the connection protocol needs next

This function handles PostgreSQL's asynchronous connection protocol, balancing connection progress with user cancellation responsiveness using a simple timeout-based approach.