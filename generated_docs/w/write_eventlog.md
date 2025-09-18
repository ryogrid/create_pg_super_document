# write_eventlog

## Location
src/backend/utils/error/elog.c: 2486 - 2575

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
  - GetACPEncoding (PostgreSQL function)
  - GetMessageEncoding (PostgreSQL function)  
  - pgwin32_message_to_UTF16 (PostgreSQL Windows utility)
  - in_error_recursion_trouble (PostgreSQL error handling)
  - pfree (PostgreSQL memory management)
  - DEFAULT_EVENT_SOURCE (constant)
- Called from (representative examples):
  - send_message_to_server_log
  - write_stderr
  - pgwin32_ServiceMain

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