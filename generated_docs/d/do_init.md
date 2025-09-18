# do_init

## Location
src/bin/pg_ctl/pg_ctl.c: 894 - 922

## Overview
Executes the database cluster initialization process by invoking the initdb command with appropriate parameters and handling the execution result.

## Definition
```c
static void do_init(void)
```

## Detailed Description
This function is responsible for initializing a new PostgreSQL database cluster through pg_ctl. It serves as a wrapper around the initdb command, handling the construction of the command line, execution, and error reporting.

The function performs the following key operations:
1. Locates the initdb executable using `find_other_exec_or_die` if not already specified
2. Constructs the appropriate command line by combining the executable path, data directory options, and any additional post options
3. Handles silent mode operation by redirecting output to the system's null device
4. Executes the initdb command using the system shell
5. Reports initialization failure and terminates on non-zero exit status

The function ensures that all necessary parameters have default values (empty strings) if not explicitly provided, making the initialization process robust against missing configuration.

## Parameters / Member Variables
This function takes no parameters but operates on several global variables:
- `exec_path`: Path to the initdb executable (determined automatically if NULL)
- `pgdata_opt`: Data directory options string (defaults to empty string)
- `post_opts`: Additional command-line options (defaults to empty string)  
- `silent_mode`: Flag controlling output verbosity
- `argv0`: Program invocation name for executable location
- `progname`: Program name for error messages

## Dependencies
- Functions called/Symbols referenced:
  - [find_other_exec_or_die](../f/find_other_exec_or_die.md) (locate initdb executable)
  - [psprintf](../p/psprintf.md) (formatted string creation)
  - `DEVNULL` (platform-specific null device path)
  - `fflush` (flush output streams before system call)
  - `system` (execute shell command)
  - [write_stderr](../w/write_stderr.md) (error output function)
  - `PG_VERSION` (PostgreSQL version constant)

- Called from:
  - [main](../m/main.md) (primary entry point for init command)

## Notes and Other Information
- The function uses `system()` to execute initdb, which invokes the command through the shell
- Silent mode redirects all output to the null device, useful for scripted operations
- The function performs a `fflush(NULL)` before system execution to ensure all buffered output is written
- Error handling is binary: success continues normally, failure terminates with exit(1)
- Version compatibility is enforced through the find_other_exec_or_die call
- The constructed command string includes proper quoting to handle paths with spaces
- Memory allocated by `psprintf` for the command string is not explicitly freed (process termination handles cleanup)