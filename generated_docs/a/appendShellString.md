# appendShellString

## Location
src/fe_utils/string_utils.c: 582 - 593

## Overview
Safely appends a string to a shell command buffer with proper quoting, terminating the program if dangerous characters (LF or CR) are detected.

## Definition
```c
void appendShellString(PQExpBuffer buf, const char *str)
```

## Detailed Description
This function provides a secure wrapper around shell command construction by appending strings with appropriate shell-style quoting to ensure they form exactly one argument. It enforces strict security by completely rejecting strings containing line feed (LF) or carriage return (CR) characters, which pose security risks and are incompatible with Windows command shells.

The function serves as a fatal-error variant of appendShellStringNoError(), prioritizing security over graceful error handling. When dangerous characters are detected, it immediately prints an error message and terminates the program with EXIT_FAILURE. This aggressive approach prevents potential security vulnerabilities from shell injection attacks that could exploit newline characters.

## Parameters / Member Variables
- `buf`: Target PQExpBuffer where the shell-quoted string will be appended
- `str`: Input string to be quoted and appended (must not contain LF or CR characters)

## Dependencies
- Functions called/Symbols referenced:
  - [appendShellStringNoError](appendShellStringNoError.md) (performs the actual quoting and validation)
  - fprintf (for error message output)
  - exit (terminates program on security violation)
  - EXIT_FAILURE (exit status constant)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_dumpall.c for command-line argument processing)
  - [ensureCleanShutdown](../e/ensureCleanShutdown.md) (in pg_rewind.c for server control commands)
  - [cluster_conn_opts](../c/cluster_conn_opts.md) (in pg_upgrade for connection parameter handling)
  - [start_standby_server](../s/start_standby_server.md) (in pg_createsubscriber.c)

## Notes and Other Information
- Designed specifically for building shell commands that will be executed via system() or similar functions
- The rejection of LF/CR characters addresses known Windows command shell limitations and general security concerns
- Used extensively in PostgreSQL utilities (initdb, pg_dump, pg_rewind, pg_upgrade) for safe command construction
- Fatal error behavior makes it unsuitable for situations requiring graceful error recovery
- Future PostgreSQL versions may reject LF/CR characters at the database level (in CREATE ROLE/DATABASE) to prevent these issues upstream
- Consider using appendShellStringNoError() if graceful error handling is required