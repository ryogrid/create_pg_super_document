# get_user_info

## Location
src/bin/pg_upgrade/util.c: 323 - 351

## Overview
get_user_info is a utility function in pg_upgrade that retrieves the current user's information, returning both the effective user ID and a dynamically allocated copy of the username.

## Definition


## Detailed Description
get_user_info serves as a wrapper function that obtains information about the currently running user for PostgreSQL's pg_upgrade utility. On Unix-like systems, it calls geteuid() to get the effective user ID, while on Windows it returns a fixed value of 1. The function then calls get_user_name() to retrieve the username string, handles any errors by calling pg_fatal(), and creates a dynamically allocated copy of the username using pg_strdup(). This function is essential for pg_upgrade to ensure proper permissions and ownership during database cluster upgrades, as the upgrade process must be run by the same user who owns the database files.

## Parameters / Member Variables
- : A pointer to a char pointer that will be set to point to a newly allocated string containing the username

## Dependencies
- Functions called/Symbols referenced:
  - geteuid (gets effective user ID on Unix systems)
  - [get_user_name](get_user_name.md) (retrieves username as a string)
  - [pg_fatal](../p/pg_fatal.md) (handles fatal errors and exits)
  - [pg_strdup](../p/pg_strdup.md) (safely duplicates strings with error handling)
- Called from (representative examples):
  - [parseCommandLine](../p/parseCommandLine.md) (during command-line processing in pg_upgrade)
  - fopen_priv (for privilege-related file operations)

## Notes and Other Information
- Platform-specific behavior: returns actual effective user ID on Unix/Linux, but hardcoded value 1 on Windows
- The returned username string is dynamically allocated and should be freed by the caller when no longer needed
- Critical for pg_upgrade's security model - ensures the upgrade runs with appropriate user privileges
- Part of pg_upgrade's cross-platform abstraction layer for user management
- The function will terminate the program with pg_fatal() if username lookup fails
- Used to verify that pg_upgrade is being run by the database owner
- Located in src/bin/pg_upgrade/util.c:323-351
- Returns integer user ID while also setting the username through the output parameter