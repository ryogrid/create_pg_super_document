# read_post_opts

## Location
[src/bin/pg_ctl/pg_ctl.c:794-848](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L794-L848)

## Overview
Reads and parses PostgreSQL server startup options from a file, specifically used during RESTART operations to preserve the original server startup configuration.

## Definition
```c
static void read_post_opts(void)
```

## Detailed Description
This function is responsible for reading PostgreSQL server startup options that were previously saved to a file (postopts_file). The function operates conditionally based on the control command being executed:

- If `post_opts` is already set (non-NULL), the function does nothing
- For non-RESTART commands, it sets `post_opts` to an empty string as default
- For RESTART commands, it reads the options file to restore the original server startup configuration

The function performs strict validation on the options file, requiring exactly one line of content. It parses the line to separate the executable path from the command-line options, using the pattern of finding the first occurrence of a space followed by a double-quote to identify where options begin.

## Parameters / Member Variables
This function takes no parameters but operates on several global variables:
- `post_opts`: Global variable storing the server startup options
- `ctl_command`: Current pg_ctl command being executed
- `postopts_file`: Path to the file containing saved startup options
- `exec_path`: Path to the PostgreSQL server executable

## Dependencies
- Functions called/Symbols referenced:
  - `RESTART_COMMAND` (constant)
  - [readfile](readfile.md) (utility function to read file contents)
  - [write_stderr](../w/write_stderr.md) (error output function)
  - [free_readfile](../f/free_readfile.md) (cleanup function)
  - [pg_strdup](../p/pg_strdup.md) (string duplication utility)

- Called from:
  - [do_start](../d/do_start.md) (main start operation function)
  - [pgwin32_ServiceMain](../p/pgwin32_ServiceMain.md) (Windows service main function)

## Notes and Other Information
- This function is critical for the RESTART operation to maintain consistency between server stops and starts
- The function expects the options file to contain exactly one line, enforcing strict format requirements
- Error handling includes immediate exit with status 1 if file reading fails or format is invalid
- The parsing logic assumes options are enclosed in double quotes and separated from the executable path by whitespace
- Memory management is handled through `free_readfile` to clean up dynamically allocated file content