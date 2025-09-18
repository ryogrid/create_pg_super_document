# report_fork_failure_to_client

## Location
[src/backend/postmaster/postmaster.c:3642-3668](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L3642-L3668)

## Overview
Attempts to send a fork failure error message to a client over their connection socket before the postmaster closes the connection, using non-blocking I/O to avoid hanging the postmaster.

## Definition
static void report_fork_failure_to_client(ClientSocket *client_sock, int errnum)

## Detailed Description
This function provides a last-ditch effort to inform a client when the postmaster fails to fork a new backend process to handle their connection. It formats an error message using the V2 protocol format, sets the client socket to non-blocking mode to prevent the postmaster from hanging, and attempts to send the error message exactly once. The function is designed to be robust and non-blocking - if setting the socket to non-blocking fails, it returns immediately without attempting to send. When sending, it retries only on EINTR but ignores all other failures, prioritizing postmaster stability over guaranteed message delivery.

## Parameters / Member Variables
- `client_sock`: Pointer to ClientSocket structure containing the client connection socket
- `errnum`: The errno value from the failed fork() operation, used to generate a descriptive error message

## Dependencies
- Functions called/Symbols referenced:
  - snprintf (formats error message)
  - strerror (converts errno to string)
  - [pg_set_noblock](../p/pg_set_noblock.md) (sets socket to non-blocking mode)
  - send (attempts to send error message)
  - strlen (calculates message length)
  - EINTR (interrupt signal constant)
- Called from (representative examples):
  - [BackendStartup](../B/BackendStartup.md) (when fork() fails during backend creation)

## Notes and Other Information
- Static function internal to postmaster.c
- Uses V2 protocol format for error messages ("E" prefix)
- Described as "grungy special-purpose code" due to its low-level nature
- Cannot use backend libpq since it's not available at this stage
- Non-blocking approach prevents postmaster from being delayed by unresponsive clients
- Only retries on EINTR (interrupted system call), ignores other errors
- Part of PostgreSQL's graceful connection handling even during failure scenarios
- Buffer size of 1000 characters should be sufficient for typical error messages
- The +1 in strlen() includes the null terminator in the transmission