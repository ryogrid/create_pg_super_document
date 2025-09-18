# set_mode

## Location
src/bin/pg_ctl/pg_ctl.c: 2035 - 2062

## Overview
A static function in pg_ctl that parses and sets the shutdown mode for PostgreSQL server operations, mapping mode strings to appropriate shutdown modes and signals.

## Definition


## Detailed Description
The  function processes shutdown mode options provided via command-line arguments (typically through the -m/--mode option). It accepts both short and long forms of shutdown mode specifications and configures the global  and  variables accordingly. The function validates the input and terminates the program with an error message if an invalid mode is provided.

The function supports three shutdown modes:
- **Smart mode** ("s" or "smart"): Sets  and  signal - waits for all clients to disconnect
- **Fast mode** ("f" or "fast"): Sets  and  signal - terminates connections and shuts down cleanly
- **Immediate mode** ("i" or "immediate"): Sets  and  signal - forces immediate shutdown without cleanup

## Parameters / Member Variables
- : String containing the shutdown mode specification (short or long form)

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (string comparison)
  - SMART_MODE, FAST_MODE, IMMEDIATE_MODE (shutdown mode constants)
  - SIGTERM, SIGINT, SIGQUIT (signal constants)
  - write_stderr (error output function)
  - do_advice (help/advice function)
  - exit (program termination)
  - shutdown_mode, sig (global variables)

- Called from (representative examples):
  - main (when processing -m/--mode command-line options)

## Notes and Other Information
- The function modifies global variables  and  that are used throughout the pg_ctl program
- Input validation terminates the program immediately on invalid modes rather than returning error codes
- Both abbreviated (s/f/i) and full (smart/fast/immediate) mode names are supported for user convenience
- Error messages are internationalized using the  macro
- Located in src/bin/pg_ctl/pg_ctl.c:2035-2062