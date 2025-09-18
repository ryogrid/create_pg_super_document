# parseCommandLine

## Location
src/bin/pg_upgrade/option.c: 39 - 281

## Overview
Parses command line arguments for the pg_upgrade utility and populates configuration structures with user-specified options.

## Definition
```c
void parseCommandLine(int argc, char *argv[])
```

## Detailed Description
This function is the core command-line argument parser for pg_upgrade, handling all configuration options that control the upgrade process. It uses getopt_long() to parse both short and long options, validating input and setting up global structures for old and new cluster information, transfer modes, and various operational flags. The function also performs environment variable processing, privilege checks, and essential validation to ensure the upgrade can proceed safely.

## Parameters / Member Variables
- `argc`: Number of command line arguments
- `argv[]`: Array of command line argument strings

## Dependencies
- Functions called/Symbols referenced:
  - [get_progname](../g/get_progname.md)
  - [get_user_info](../g/get_user_info.md)
  - [usage](../u/usage.md)
  - getopt_long
  - [pg_strdup](pg_strdup.md)
  - [pg_free](pg_free.md)
  - [pg_log](pg_log.md)
  - [check_required_directory](../c/check_required_directory.md)
  - [parse_sync_method](parse_sync_method.md)
  - setenv
  - [canonicalize_path](../c/canonicalize_path.md) (Windows)
  - [path_is_prefix_of_path](path_is_prefix_of_path.md) (Windows)
- Called from (representative examples):
  - [main](../m/main.md) (src/bin/pg_upgrade/pg_upgrade.c:103)

## Notes and Other Information
- Supports comprehensive set of options including data directories (-d/-D), binary directories (-b/-B), ports (-p/-P), transfer modes (--link, --clone, --copy), and operational flags
- Performs security check to prevent running as root user
- Handles environment variables (PGPORTOLD, PGPORTNEW, PGUSER, PGOPTIONS) with proper defaults
- On Windows, includes special validation to prevent running from inside the new cluster directory
- Sets up PGOPTIONS environment variable with FIX_DEFAULT_READ_ONLY to handle read-only mode
- Uses check_required_directory to validate and set directory paths from command line or environment variables
- Transfer modes include COPY (default), LINK, CLONE, and COPY_FILE_RANGE options