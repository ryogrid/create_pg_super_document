# make_temp_sockdir

## Location
[src/test/regress/pg_regress.c:500-540](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L500-L540)

## Overview
Creates a secure temporary directory for PostgreSQL's Unix-domain socket during regression testing, with proper permissions and signal-based cleanup handling.

## Definition


## Detailed Description
This function creates a temporary directory specifically for storing Unix-domain socket files during pg_regress operations. The directory is created with restrictive permissions (mode 0700) to prevent other OS users from accessing the socket and potentially exploiting trust authentication. The function uses a template-based approach with mkdtemp() for secure directory creation, sets up cleanup handlers for both normal and signal-based termination, and prepares socket file paths for later use. The directory is placed under TMPDIR (or /tmp) rather than the current working directory to avoid path length constraints and ensure writability.

## Parameters / Member Variables
- No parameters (void function)
- Returns:  - pointer to the created temporary directory path

## Dependencies
- Functions called/Symbols referenced:
  - [psprintf](../p/psprintf.md)
  - getenv
  - [mkdtemp](mkdtemp.md) (system call)
  - bail
  - UNIXSOCK_PATH (macro)
  - snprintf
  - atexit
  - [remove_temp](../r/remove_temp.md)
  - [pqsignal](../p/pqsignal.md)
  - [signal_remove_temp](../s/signal_remove_temp.md)
- Called from (representative examples):
  - [initialize_environment](../i/initialize_environment.md)

## Notes and Other Information
- Function is marked static (internal to pg_regress.c)
- Uses TMPDIR environment variable if set, otherwise defaults to "/tmp"
- Creates directory with template "pg_regress-XXXXXX" for uniqueness
- Directory permissions are 0700 or stricter for security
- Sets up both atexit() and signal handlers for cleanup
- Signal handlers installed for: SIGHUP, SIGINT, SIGPIPE, SIGTERM
- SIGQUIT is intentionally omitted to preserve it as a quick, untidy exit
- Prepares global variables sockself and socklock for later cleanup
- Addresses path length constraints of Unix socket paths
- Enables testing in builds where DEFAULT_PGSOCKET_DIR is not writable
- Located in src/test/regress/pg_regress.c:500-540