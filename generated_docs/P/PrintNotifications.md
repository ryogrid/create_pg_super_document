# PrintNotifications

## Location
[src/bin/psql/common.c:705-737](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L705-L737)

## Overview
PrintNotifications is a static helper function that checks for and displays any pending asynchronous notifications from the PostgreSQL server in a formatted, user-friendly manner.

## Definition

```c
static void
PrintNotifications(void)
```
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



## Dependencies
- Functions called/Symbols referenced:
  - PGnotify (libpq structure type for notification data)
  - [PQconsumeInput](PQconsumeInput.md) (consumes input from server connection)
  - [PQnotifies](PQnotifies.md) (retrieves pending notifications)
  - [PQfreemem](PQfreemem.md) (frees notification memory)

- Called from (representative examples):
  - [SendQuery](../S/SendQuery.md) (processes notifications after query execution)

## Notes and Other Information
- This is a static function, only accessible within src/bin/psql/common.c
- Uses pset.queryFout for output, allowing redirection to files or other streams
- Displays notification channel name, payload (if present), and sender process ID
- Implements backward compatibility by conditionally showing payload
- Ensures proper cleanup by calling PQfreemem() for each processed notification
- Calls PQconsumeInput() both before processing and after each notification to ensure all data is retrieved
- Uses gettext (_) macro for internationalized notification messages
- Flushes output after each notification to ensure immediate display

## Simplified Source

```c
static void PrintNotifications(void) {
    PGnotify *notify;

    // Consume input and process all pending notifications
    PQconsumeInput(pset.db);
    while ((notify = PQnotifies(pset.db)) != NULL) {
        // Display notification with or without payload
        if (notify->extra[0]) {
            fprintf(pset.queryFout, "Asynchronous notification \"%s\" with payload \"%s\" received from server process with PID %d.\n",
                    notify->relname, notify->extra, notify->be_pid);
        } else {
            fprintf(pset.queryFout, "Asynchronous notification \"%s\" received from server process with PID %d.\n",
                    notify->relname, notify->be_pid);
        }

        // Clean up and continue processing
        fflush(pset.queryFout);
        PQfreemem(notify);
        PQconsumeInput(pset.db);
    }
}
```

This simplified version preserves the core functionality: consuming server input, processing all notifications in a loop, displaying them with optional payload information, and proper memory cleanup.