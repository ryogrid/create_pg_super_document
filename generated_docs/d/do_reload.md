# do_reload

## Location
src/bin/pg_ctl/pg_ctl.c: 1137 - 1173

## Overview
Sends a reload signal to a running PostgreSQL server to trigger reloading of configuration files without requiring a full restart.

## Definition
```c
static void
do_reload(void)
```

## Detailed Description
The `do_reload` function provides a mechanism to instruct a running PostgreSQL server to reload its configuration files (postgresql.conf, pg_hba.conf, etc.) without interrupting active connections or requiring a full server restart. This is accomplished by sending a SIGHUP signal to the postmaster process.

The function performs several validation checks before sending the signal:
- Ensures the server is actually running by checking for a valid PID file
- Prevents reload attempts on standalone backends, which don't support configuration reloading
- Validates that the process exists and the signal can be delivered

Unlike restart operations, reload is a lightweight operation that allows configuration changes to take effect while maintaining server availability. However, not all configuration parameters can be changed through reload - some require a full server restart.

## Parameters / Member Variables
This function takes no parameters and operates on global variables:
- Uses global `sig` variable (typically SIGHUP for reload operations)
- Uses global `progname` and `pid_file` for error reporting and PID file location

## Dependencies
- Functions called/Symbols referenced:
  - [get_pgpid](../g/get_pgpid.md) - retrieves the postmaster process ID from the PID file
  - kill - sends the reload signal (SIGHUP) to the postmaster process
  - [write_stderr](../w/write_stderr.md) - outputs error messages for various failure conditions
  - [print_msg](../p/print_msg.md) - outputs success confirmation message

- Called from (representative examples):
  - [main](../m/main.md) - [main](../m/main.md) entry point of pg_ctl when reload action is requested

## Notes and Other Information
- The reload operation sends SIGHUP signal, which is the standard Unix signal for configuration reload
- Only configuration parameters marked as PGC_SIGHUP can be changed through reload; others require restart
- Standalone backends cannot be reloaded as they don't support dynamic configuration changes
- The function provides immediate feedback but doesn't wait for the reload process to complete
- Configuration reload errors (if any) will appear in the server logs, not in pg_ctl output
- This is the safest way to apply configuration changes without service interruption
- Error messages are internationalized using the gettext system