# handle_args

## Location
[src/bin/pg_test_fsync/pg_test_fsync.c:148-230](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_test_fsync/pg_test_fsync.c#L148-L230)

## Overview
The handle_args function parses command-line arguments for the pg_test_fsync utility, processing options for filename specification and test duration configuration.

## Definition
```c
static void handle_args(int argc, char *argv[])
```

## Detailed Description
This function handles command-line argument parsing for the pg_test_fsync utility using getopt_long. It supports standard PostgreSQL command-line conventions including --help and --version options. The function processes two main options: filename specification (-f/--filename) and test duration (-s/--secs-per-test). It performs input validation, error handling, and displays platform-specific information about direct I/O support. The function exits the program on help/version requests or validation errors.

## Parameters / Member Variables
- `argc`: Number of command-line arguments
- `argv[]`: Array of command-line argument strings

## Dependencies
- Functions called/Symbols referenced:
  - [getopt_long](../g/getopt_long.md) (GNU option parsing)
  - [pg_strdup](../p/pg_strdup.md) (PostgreSQL string duplication)
  - strtoul (string to unsigned long conversion)
  - pg_log_error (PostgreSQL error logging)
  - pg_log_error_hint (PostgreSQL error hint logging)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL fatal error)
  - ngettext (internationalization pluralization)
  - O_DIRECT (direct I/O flag checking)
- Called from (representative examples):
  - [main](../m/main.md) (pg_test_fsync main function)
  - [main](../m/main.md) (pg_test_timing main function)

## Notes and Other Information
- Supports both short (-f, -s) and long (--filename, --secs-per-test) option formats
- Validates that secs-per-test is a positive integer within valid range
- Displays platform-specific direct I/O support information (O_DIRECT, F_NOCACHE, or none)
- Uses PostgreSQL's standard logging and error handling functions
- Supports internationalization through ngettext for plural forms
- Exits with status 0 for help/version, status 1 for errors
- File location: src/bin/pg_test_fsync/pg_test_fsync.c:148-230