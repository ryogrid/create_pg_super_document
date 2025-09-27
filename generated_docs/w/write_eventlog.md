# write_eventlog

## Location
[src/backend/utils/error/elog.c:2486-2575](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L2486-L2575)

## Overview
The write_eventlog function writes PostgreSQL log messages to the Windows Event Log, handling character encoding conversion and mapping PostgreSQL log levels to Windows event types.

## Definition
```c
static void write_eventlog(int level, const char *line, int len)
```

## Detailed Description
This Windows-specific function provides integration with the Windows Event Log system. It performs several key operations:

1. **Event source registration**: Lazily registers with Windows Event Log using RegisterEventSource() on first use
2. **Log level mapping**: Maps PostgreSQL log levels (DEBUG, INFO, WARNING, ERROR, etc.) to Windows event types (INFORMATION, WARNING, ERROR)
3. **Character encoding handling**: Intelligently handles character encoding by:
   - Using ReportEventA() when message encoding matches the Windows ACP encoding
   - Converting to UTF-16 and using ReportEventW() when encodings don't match
   - Falling back to ReportEventA() if conversion fails or memory contexts aren't available
4. **Error recursion protection**: Includes safeguards against infinite recursion during error reporting

The function ensures that PostgreSQL log messages are properly formatted and encoded for the Windows Event Log system.

## Parameters / Member Variables
- : PostgreSQL log level (DEBUG5, LOG, WARNING, ERROR, etc.)
- : The log message string to be written
- : Length of the message string

## Dependencies
- Functions called/Symbols referenced:
  - RegisterEventSource (Windows API)
  - ReportEventA, ReportEventW (Windows API)
  - [GetACPEncoding](../G/GetACPEncoding.md) (PostgreSQL function)
  - [GetMessageEncoding](../G/GetMessageEncoding.md) (PostgreSQL function)  
  - [pgwin32_message_to_UTF16](../p/pgwin32_message_to_UTF16.md) (PostgreSQL Windows utility)
  - [in_error_recursion_trouble](../i/in_error_recursion_trouble.md) (PostgreSQL error handling)
  - [pfree](../p/pfree.md) (PostgreSQL memory management)
  - DEFAULT_EVENT_SOURCE (constant)
- Called from (representative examples):
  - [send_message_to_server_log](../s/send_message_to_server_log.md)
  - [write_stderr](write_stderr.md)
  - [pgwin32_ServiceMain](../p/pgwin32_ServiceMain.md)

## Notes and Other Information
- Windows-specific function, only compiled and used on Windows platforms
- Uses static HANDLE evtHandle for caching the event log handle
- Maps all PostgreSQL DEBUG levels and INFO/NOTICE to EVENTLOG_INFORMATION_TYPE
- Maps WARNING levels to EVENTLOG_WARNING_TYPE
- Maps ERROR/FATAL/PANIC to EVENTLOG_ERROR_TYPE
- All events are logged with ID 0 in the Windows Event Log
- Includes sophisticated encoding detection and conversion logic to handle international character sets properly
- Part of PostgreSQL's Windows-specific logging infrastructure in src/backend/utils/error/elog.c
- Falls back gracefully when memory allocation or encoding conversion fails

## Simplified Source

```c
// Simplified version of write_eventlog
static void write_eventlog(int log_level, const char *message, int message_len) {
    static HANDLE event_handle = INVALID_HANDLE_VALUE;
    int windows_event_type = EVENTLOG_ERROR_TYPE;

    // Step 1: Initialize Windows Event Log handle on first use
    if (event_handle == INVALID_HANDLE_VALUE) {
        event_handle = RegisterEventSource(NULL, event_source ? event_source : DEFAULT_EVENT_SOURCE);
        if (event_handle == NULL) {
            event_handle = INVALID_HANDLE_VALUE;
            return;  // Failed to register, cannot log
        }
    }

    // Step 2: Map PostgreSQL log levels to Windows event types
    if (log_level <= NOTICE) {
        windows_event_type = EVENTLOG_INFORMATION_TYPE;  // DEBUG*, LOG, INFO, NOTICE
    } else if (log_level <= WARNING_CLIENT_ONLY) {
        windows_event_type = EVENTLOG_WARNING_TYPE;      // WARNING*
    } else {
        windows_event_type = EVENTLOG_ERROR_TYPE;        // ERROR, FATAL, PANIC
    }

    // Step 3: Handle character encoding and write to event log
    if (encoding_conversion_safe() && message_needs_utf16_conversion()) {
        // Try UTF-16 conversion for international characters
        WCHAR *utf16_message = pgwin32_message_to_UTF16(message, message_len, NULL);
        if (utf16_message) {
            ReportEventW(event_handle, windows_event_type, 0, 0, NULL, 1, 0,
                        (LPCWSTR *) &utf16_message, NULL);
            pfree(utf16_message);
            return;
        }
    }

    // Step 4: Fallback to ASCII reporting
    ReportEventA(event_handle, windows_event_type, 0, 0, NULL, 1, 0, &message, NULL);
}

// Helper function for readability
static bool encoding_conversion_safe() {
    return !in_error_recursion_trouble() && CurrentMemoryContext != NULL;
}

static bool message_needs_utf16_conversion() {
    return GetMessageEncoding() != GetACPEncoding();
}
```

Key simplifications made:
- Consolidated the complex switch statement into a simpler if-else chain
- Extracted encoding safety checks into helper functions for clarity
- Removed detailed comments about Windows API specifics
- Simplified variable names for better readability
- Abstracted the complex encoding detection logic
- Focused on the main execution path: register handle, map levels, handle encoding, write event