# check_ok

## Location
[src/bin/initdb/initdb.c:2091-2115](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L2091-L2115)

## Overview
A utility function that checks for error conditions and either exits with an error message or prints "ok" to indicate successful completion of an operation.

## Definition
```c
static void check_ok(void)
```

## Detailed Description
The `check_ok` function serves as a centralized error checking and status reporting mechanism throughout the initdb and pg_upgrade processes. It examines two global error conditions:

1. **Signal interruption**: Checks if a signal was caught (via the `caught_signal` flag set by `trapsig`)
2. **Output failure**: Checks if there was a failure writing to a child process (via `output_failed` flag)

If either error condition is detected, the function prints an appropriate error message and exits with status 1. If no errors are detected, it prints "ok" to indicate successful completion and flushes stdout to ensure immediate output visibility.

This function is commonly called after major initialization steps to provide user feedback and ensure proper error handling throughout the database initialization process.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - printf (for output messages)
  - fflush (to ensure immediate output)
  - exit (to terminate on errors)
  - strerror (to convert errno to readable error message)
  - \_ (gettext macro for internationalization)

- Global variables accessed:
  - caught_signal (boolean flag set by signal handler)
  - output_failed (boolean flag indicating output errors)
  - output_errno (errno value from failed output operation)

- Called from (representative examples):
  - [setup_config](../s/setup_config.md) (after configuration setup)
  - [bootstrap_template1](../b/bootstrap_template1.md) (after template1 creation)
  - [create_data_directory](create_data_directory.md) (after directory creation)
  - [initialize_data_directory](../i/initialize_data_directory.md) (after various initialization steps)
  - Multiple pg_upgrade functions for validation steps

## Notes and Other Information
- Provides consistent error handling and user feedback across initdb and pg_upgrade
- Uses internationalized messages for better user experience
- Flushes stdout to ensure immediate visibility of status messages
- Centralized approach allows uniform error reporting throughout the codebase
- Exit status 1 is used for all error conditions to indicate failure to shell/scripts
- The "ok" message provides positive feedback to users during long-running operations

## Simplified Source

```c
static void check_ok(void) {
    if (caught_signal) {
        printf(_("caught signal\n"));
        fflush(stdout);
        exit(1);
    } else if (output_failed) {
        printf(_("could not write to child process: %s\n"), strerror(output_errno));
        fflush(stdout);
        exit(1);
    } else {
        // Success case
        printf(_("ok\n"));
        fflush(stdout);
    }
}
```