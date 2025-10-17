# do_reload

## Location
[src/bin/pg_ctl/pg_ctl.c:1137-1173](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L1137-L1173)

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

## Simplified Source

```c
static void
do_reload(void)
{
    pid_t pid;

    // Get the postmaster process ID from PID file
    pid = get_pgpid(false);

    // Validate server state
    if (pid == 0) {
        // No PID file exists
        write_stderr("PID file does not exist\n");
        write_stderr("Is server running?\n");
        exit(1);
    } else if (pid < 0) {
        // Standalone backend cannot be reloaded
        pid = -pid;
        write_stderr("Cannot reload single-user server (PID: %d)\n", (int) pid);
        write_stderr("Please terminate the single-user server and try again.\n");
        exit(1);
    }

    // Send reload signal (SIGHUP) to postmaster
    if (kill(pid, sig) != 0) {
        write_stderr("Could not send reload signal (PID: %d)\n", (int) pid);
        exit(1);
    }

    // Confirm signal sent
    print_msg("server signaled\n");
}
```