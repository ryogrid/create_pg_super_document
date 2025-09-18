# PQsetNoticeProcessor

## Location
src/interfaces/libpq/fe-connect.c: 7338 - 7360

## Overview
Sets a callback function to process notice messages from the PostgreSQL server as plain text strings, providing a simpler alternative to PQsetNoticeReceiver for handling server notices.

## Definition


Where PQnoticeProcessor is defined as:


## Detailed Description
PQsetNoticeProcessor installs a callback function that receives notice messages from the PostgreSQL server as formatted text strings. Unlike PQsetNoticeReceiver which provides the full PGresult structure, this function provides a simplified interface where notices are delivered as pre-formatted strings. This is particularly useful for applications that need to log or display notices without needing to parse the detailed structure of the notice message.

## Parameters / Member Variables
- : The database connection handle (if NULL, returns NULL)
- : Pointer to the callback function that will process notice messages (or NULL to disable notice processing)
- : User-defined argument that will be passed to the callback function

## Dependencies
- Functions called/Symbols referenced:
  - PQnoticeProcessor (function pointer type)
- Called from (representative examples):
  - ConnectDatabase (in pg_backup_db.c)
  - do_connect (in psql command.c)
  - main (in isolationtester.c)
  - test_pipeline_idle (in libpq_pipeline.c)

## Notes and Other Information
- Returns the previous notice processor function pointer, allowing for restoration
- If conn is NULL, the function returns NULL safely
- The callback function receives the user-defined arg and a formatted message string
- Setting proc to NULL disables notice processing for the connection
- This provides a simpler interface compared to PQsetNoticeReceiver for basic notice handling
- The message string is pre-formatted and ready for display or logging
- Applications should avoid performing lengthy operations in the notice processor callback
- Commonly used by command-line tools like pg_dump and psql for notice logging
- The message string should not be modified or freed by the application
- Notice processing is synchronous and occurs when notices are received from the server