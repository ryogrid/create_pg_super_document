# LockTimeoutHandler

## Location
src/backend/utils/init/postinit.c: 1400 - 1409

## Overview
LockTimeoutHandler is a signal handler function that responds to lock timeout events by sending a query cancellation interrupt to terminate lock waits that exceed the configured timeout period.

## Definition
```c
static void LockTimeoutHandler(void)
```

## Detailed Description
This function serves as the timeout handler for lock acquisition timeouts in PostgreSQL. When a process waits for a lock longer than the configured lock_timeout duration, this handler is invoked to interrupt the lock wait operation. Unlike StatementTimeoutHandler, this function always sends SIGINT regardless of the authentication state, as lock timeouts only occur during normal database operations, not during authentication.

The function attempts to signal both the individual process and the entire process group (when HAVE_SETSID is available), ensuring that any related processes are also notified of the lock timeout condition. This helps prevent deadlock situations and ensures that lock waits don't block the system indefinitely.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - kill (system call for sending signals)
- Called from (representative examples):
  - InitPostgres (src/backend/utils/init/postinit.c:774)

## Notes and Other Information
- Always sends SIGINT, never SIGTERM, as lock timeouts are recoverable conditions
- Uses conditional compilation with HAVE_SETSID for process group signaling on compatible systems
- This is a static function, used only within the postinit.c module
- Lock timeouts help prevent deadlocks and ensure system responsiveness by limiting how long processes wait for locks
- The handler is typically registered during backend initialization and triggered by the timeout management system
- Unlike statement timeouts, lock timeouts are specifically designed to be non-fatal, allowing the transaction to continue with appropriate error handling