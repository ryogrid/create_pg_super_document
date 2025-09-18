# PrintNotifications

## Location
src/bin/psql/common.c: 705 - 737

## Overview
PrintNotifications is a static helper function that checks for and displays any pending asynchronous notifications from the PostgreSQL server in a formatted, user-friendly manner.

## Definition


## Detailed Description
PrintNotifications handles the processing and display of asynchronous notifications sent by the PostgreSQL server via the NOTIFY/LISTEN mechanism. The function:

- Consumes any available input from the server using PQconsumeInput()
- Retrieves all pending notifications using PQnotifies() in a loop
- Formats and displays each notification with appropriate details
- Supports both simple notifications and notifications with payload data
- Maintains backward compatibility by only showing payload when present
- Properly manages memory by freeing notification structures after processing
- Outputs to the configured query output stream (pset.queryFout)

The function ensures all notifications are processed and displayed immediately, providing real-time feedback for applications using PostgreSQL's asynchronous messaging features.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - PGnotify (libpq structure type for notification data)
  - PQconsumeInput (consumes input from server connection)
  - PQnotifies (retrieves pending notifications)
  - PQfreemem (frees notification memory)

- Called from (representative examples):
  - SendQuery (processes notifications after query execution)

## Notes and Other Information
- This is a static function, only accessible within src/bin/psql/common.c
- Uses pset.queryFout for output, allowing redirection to files or other streams
- Displays notification channel name, payload (if present), and sender process ID
- Implements backward compatibility by conditionally showing payload
- Ensures proper cleanup by calling PQfreemem() for each processed notification
- Calls PQconsumeInput() both before processing and after each notification to ensure all data is retrieved
- Uses gettext (_) macro for internationalized notification messages
- Flushes output after each notification to ensure immediate display