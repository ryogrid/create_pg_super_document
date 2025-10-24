# get_user_name_or_exit

## Location
[src/common/username.c:74-87](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/username.c#L74-L87)

## Overview
A convenience wrapper around `get_user_name` that terminates the program with an error message if user name lookup fails.

## Definition
```c
const char *get_user_name_or_exit(const char *progname)
```

## Detailed Description
This function provides a simplified interface for retrieving the current user name when failure is not expected to be recoverable. It calls `get_user_name` internally and handles any errors by printing an error message to stderr and terminating the program with exit code 1. This approach is commonly used in PostgreSQL command-line utilities where user name lookup failure represents a fatal configuration or system issue that prevents the program from continuing.

The function follows PostgreSQL's convention of including the program name in error messages to help users identify which tool encountered the problem.

## Parameters / Member Variables
- `progname`: The name of the calling program, used in error messages to identify the source of the error

## Dependencies
- Functions called/Symbols referenced:
  - [get_user_name](get_user_name.md)
  - `fprintf`
  - `exit`
- Called from (representative examples):
  - [main](../m/main.md) (src/backend/main/main.c:197)
  - [get_id](get_id.md) (src/bin/initdb/initdb.c:825)
  - [main](../m/main.md) (src/bin/pg_amcheck/pg_amcheck.c:510)
  - [main](../m/main.md) (src/bin/pgbench/pgbench.c:7103)
  - [main](../m/main.md) (src/bin/scripts/clusterdb.c:165)
  - [main](../m/main.md) (src/bin/scripts/createdb.c:185)
  - [main](../m/main.md) (src/bin/scripts/createuser.c:225)
  - [main](../m/main.md) (src/bin/scripts/reindexdb.c:235)
  - [main](../m/main.md) (src/bin/scripts/vacuumdb.c:395)

## Notes and Other Information
- This function is widely used across PostgreSQL command-line utilities as a standard way to obtain the current user name
- The function never returns NULL - it either returns a valid user name or terminates the program
- Error messages are written to stderr to follow Unix conventions for error reporting
- The program exits with status code 1 to indicate failure to the parent process or shell
- This function is not suitable for library code or server processes where graceful error handling is required

## Simplified Source

```c
const char *
get_user_name_or_exit(const char *progname)
{
    char *errstr;

    // Try to get user name
    const char *user_name = get_user_name(&errstr);

    // If it failed, print error and exit
    if (!user_name) {
        fprintf(stderr, "%s: %s\n", progname, errstr);
        exit(1);
    }

    return user_name;
}
```