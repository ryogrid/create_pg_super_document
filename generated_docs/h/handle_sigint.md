# handle_sigint

## Location
[src/fe_utils/cancel.c:153-182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/cancel.c#L153-L182)

## Overview
handle_sigint is a signal handler function that responds to interrupt signals (typically Ctrl+C) by attempting to cancel the currently executing database query.

## Definition

```c
static void
handle_sigint(SIGNAL_ARGS)
```
## Detailed Description
handle_sigint is a static signal handler function designed to handle SIGINT (interrupt) signals, typically generated when a user presses Ctrl+C. When invoked, it sets a global flag indicating that a cancel was requested and attempts to cancel any currently running database query using the global cancelConn object.

The function first sets the CancelRequested flag to true, then calls a user-defined callback function if one is registered. If a cancel connection is available (cancelConn is not NULL), it uses PQcancel to send a cancellation request to the PostgreSQL server. The function provides user feedback by writing appropriate messages to stderr, indicating whether the cancellation was sent successfully or failed.

## Parameters / Member Variables
- Uses SIGNAL_ARGS macro for signal handler parameters (typically int signum)

## Dependencies
- Functions called/Symbols referenced:
  - [PQcancel](../P/PQcancel.md)
  - [write_stderr](../w/write_stderr.md)
  - SIGNAL_ARGS (macro)
- Called from (representative examples):
  - No direct callers found (signal handler registered with operating system)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same source file
- Uses a 256-byte error buffer to capture potential error messages from PQcancel
- Sets the global CancelRequested variable to indicate a cancellation was attempted
- Calls an optional cancel_callback function if registered, allowing for custom cleanup logic
- Provides user feedback through predefined messages (cancel_sent_msg, cancel_not_sent_msg)
- Must be registered as a signal handler using platform-specific signal handling functions
- Safe to call even when cancelConn is NULL - includes null check before attempting cancellation