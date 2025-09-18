# set_sig

## Location
src/bin/pg_ctl/pg_ctl.c: 2063 - 2091

## Overview
A static function in pg_ctl that parses signal name strings and sets the global signal variable for the kill command functionality.

## Definition


## Detailed Description
The  function converts string representations of signal names into their corresponding signal constants and stores the result in the global  variable. This function is specifically used by the pg_ctl kill command to allow users to specify signals by name rather than numeric values. It validates the input signal name and terminates the program with an error message if an unrecognized signal is provided.

The function supports the following signals that are commonly used for PostgreSQL process management:
- **HUP**: Hangup signal (SIGHUP) - typically used for configuration reload
- **INT**: Interrupt signal (SIGINT) - graceful shutdown request  
- **QUIT**: Quit signal (SIGQUIT) - immediate shutdown
- **ABRT**: Abort signal (SIGABRT) - abnormal termination
- **KILL**: Kill signal (SIGKILL) - forced termination (cannot be caught)
- **TERM**: Terminate signal (SIGTERM) - polite shutdown request
- **USR1**: User-defined signal 1 (SIGUSR1) - application-specific use
- **USR2**: User-defined signal 2 (SIGUSR2) - application-specific use

## Parameters / Member Variables
- : String containing the signal name (without SIG prefix, e.g., "HUP", "TERM")

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (string comparison)
  - SIGHUP, SIGINT, SIGQUIT, SIGABRT, SIGKILL, SIGTERM, SIGUSR1, SIGUSR2 (signal constants)
  - write_stderr (error output function)
  - do_advice (help/advice function)
  - exit (program termination)
  - sig (global variable)

- Called from (representative examples):
  - main (when processing kill command with signal names)

## Notes and Other Information
- The function modifies the global  variable that is used by the kill functionality
- Signal names are case-sensitive and must match exactly (uppercase)
- The "SIG" prefix is not required in the input (e.g., use "HUP" not "SIGHUP")
- Error handling terminates the program immediately rather than returning error codes
- This function enables the pg_ctl kill command syntax: 
- Error messages are internationalized using the  macro
- Located in src/bin/pg_ctl/pg_ctl.c:2063-2091