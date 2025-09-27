# pg_console_handler

## Location
[src/backend/port/win32/signal.c:377-388](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/win32/signal.c#L377-L388)

## Overview
A Windows console control handler that converts console events (Ctrl+C, Ctrl+Break, close, shutdown) into PostgreSQL SIGINT signals.

## Definition

```c
static BOOL WINAPI
pg_console_handler(DWORD dwCtrlType)
```
## Detailed Description
This function serves as a Windows console control handler that intercepts console control events and translates them into PostgreSQL's internal signal system. It handles various console events including Ctrl+C, Ctrl+Break, console window close, and system shutdown events by converting them all to SIGINT signals.

The function executes on a thread created by the Windows operating system at the time of event invocation, making it part of the Windows signal emulation architecture. When any of the handled console events occurs, the function queues a SIGINT signal using the PostgreSQL signal queuing mechanism and returns TRUE to indicate that the event was handled.

## Parameters / Member Variables
- : A DWORD value indicating the type of console control event that occurred. Supported values include CTRL_C_EVENT, CTRL_BREAK_EVENT, CTRL_CLOSE_EVENT, and CTRL_SHUTDOWN_EVENT.

## Dependencies
- Functions called/Symbols referenced:
  - [pg_queue_signal](pg_queue_signal.md)
- Constants referenced:
  - CTRL_C_EVENT (Windows API)
  - CTRL_BREAK_EVENT (Windows API)
  - CTRL_CLOSE_EVENT (Windows API)
  - CTRL_SHUTDOWN_EVENT (Windows API)
  - SIGINT
- Called from (representative examples):
  - [pgwin32_signal_initialize](pgwin32_signal_initialize.md) (in signal.c:109) - for registration

## Notes and Other Information
- Executes on an OS-created thread, requiring careful synchronization considerations
- Uses WINAPI calling convention as required for Windows console handlers
- Returns TRUE for handled events (CTRL_C, CTRL_BREAK, CTRL_CLOSE, CTRL_SHUTDOWN)
- Returns FALSE for unhandled events, allowing other handlers to process them
- All handled console events are converted to SIGINT rather than different signal types
- Registered with the system via SetConsoleCtrlHandler() during signal initialization
- Provides a bridge between Windows console events and PostgreSQL's Unix-like signal system
- Essential for proper signal handling in PostgreSQL console applications on Windows

## Simplified Source

```c
// Simplified version of pg_console_handler
// Windows console control handler - converts console events to SIGINT signals
static BOOL WINAPI pg_console_handler(DWORD dwCtrlType) {
    // Check if this is one of the console events we handle
    if (dwCtrlType == CTRL_C_EVENT ||       // Ctrl+C pressed
        dwCtrlType == CTRL_BREAK_EVENT ||   // Ctrl+Break pressed
        dwCtrlType == CTRL_CLOSE_EVENT ||   // Console window closed
        dwCtrlType == CTRL_SHUTDOWN_EVENT)  // System shutdown
    {
        // Convert all these events to SIGINT signal
        pg_queue_signal(SIGINT);
        return TRUE;  // Event handled
    }

    // Unknown event - let other handlers process it
    return FALSE;
}
```

Key simplifications made:
- Added descriptive comments for each console event type
- Clarified the purpose of return values (TRUE/FALSE)
- Maintained the original logic flow without changes
- Focused on the core functionality: event detection and signal queuing