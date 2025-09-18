# CtlCommand

## Location
src/bin/pg_ctl/pg_ctl.c: 66 - 67

## Overview
Enumeration type that defines the available commands for the PostgreSQL control utility (pg_ctl), used to manage PostgreSQL server instances.

## Definition


## Detailed Description
The CtlCommand enum represents all possible operations that the pg_ctl utility can perform on a PostgreSQL server instance. This enumeration is used internally by pg_ctl to track which command the user has requested via command-line arguments. Each enum value corresponds to a specific administrative action that can be performed on a PostgreSQL database cluster, from basic lifecycle management (start, stop, restart) to more specialized operations (promote, logrotate, Windows service registration).

The enum values are processed in a switch statement in the main function, where each command dispatches to its corresponding handler function (do_start(), do_stop(), etc.).

## Parameters / Member Variables
- **NO_COMMAND**: Default value indicating no command has been specified
- **INIT_COMMAND**: Initialize a new PostgreSQL database cluster
- **START_COMMAND**: Start the PostgreSQL server
- **STOP_COMMAND**: Stop the PostgreSQL server
- **RESTART_COMMAND**: Restart the PostgreSQL server (stop then start)
- **RELOAD_COMMAND**: Reload the server configuration files
- **STATUS_COMMAND**: Display the current status of the PostgreSQL server
- **PROMOTE_COMMAND**: Promote a standby server to primary
- **LOGROTATE_COMMAND**: Rotate the PostgreSQL log files
- **KILL_COMMAND**: Forcefully terminate PostgreSQL processes
- **REGISTER_COMMAND**: Register PostgreSQL as a Windows service (Windows only)
- **UNREGISTER_COMMAND**: Unregister PostgreSQL Windows service (Windows only)
- **RUN_AS_SERVICE_COMMAND**: Run PostgreSQL as a Windows service (Windows only)

## Dependencies
- Functions called/Symbols referenced:
  - Used in switch statement at main() function
  - Assigned from command-line argument parsing logic
- Called from (representative examples):
  - main() function in pg_ctl.c:2459
  - Command-line parsing logic in pg_ctl.c:2356-2389

## Notes and Other Information
- Defined in src/bin/pg_ctl/pg_ctl.c:51-66
- Used by the static variable  which tracks the current operation
- Three enum values (REGISTER_COMMAND, UNREGISTER_COMMAND, RUN_AS_SERVICE_COMMAND) are specific to Windows platforms
- The enum is processed in the main switch statement to dispatch to appropriate handler functions
- NO_COMMAND serves as both initialization value and error state indicator when no valid command is provided