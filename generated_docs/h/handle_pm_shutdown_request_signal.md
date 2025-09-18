# handle_pm_shutdown_request_signal

## Location
src/backend/postmaster/postmaster.c: 2169 - 2192

## Overview
A signal handler function that processes shutdown request signals (SIGTERM, SIGINT, SIGQUIT) sent to the PostgreSQL postmaster process, setting appropriate flags to indicate the type of shutdown requested.

## Definition


## Detailed Description
This signal handler function is responsible for interpreting different shutdown signals sent to the PostgreSQL postmaster and setting corresponding internal flags to indicate the type of shutdown requested. The function supports three types of shutdown requests:

- **SIGTERM**: Triggers a smart shutdown (waits for existing connections to complete)
- **SIGINT**: Triggers a fast shutdown (terminates existing connections)  
- **SIGQUIT**: Triggers an immediate shutdown (terminates immediately without cleanup)

The function sets boolean flags that are later processed by the main postmaster loop to execute the appropriate shutdown sequence. After setting the flags, it wakes up the main postmaster loop by setting a latch.

## Parameters / Member Variables
- : Standard PostgreSQL signal handler argument macro that provides access to  containing the signal number

## Dependencies
- Functions called/Symbols referenced:
  -  - Wakes up the postmaster main loop
  -  - Signal handler argument macro
  -  - Signal constant
- Called from (representative examples):
  -  - Registered as signal handler for SIGTERM, SIGINT, SIGQUIT

## Notes and Other Information
- This is a static function within postmaster.c, indicating it's only used internally by the postmaster module
- The function uses a switch statement to handle different signal types and sets multiple boolean flags that are checked by the main postmaster loop
- The pg_ctl utility uses these three signals to request different shutdown modes
- The function follows PostgreSQL's signal handling pattern of doing minimal work in the signal handler and deferring actual processing to the main event loop