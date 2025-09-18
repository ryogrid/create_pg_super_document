# output_completion_banner

## Location
[src/bin/pg_upgrade/check.c:762-795](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L762-L795)

## Overview
Displays the final success message and post-upgrade instructions to the user upon completion of the pg_upgrade process.

## Definition


## Detailed Description
This function generates and displays the completion banner that appears when pg_upgrade finishes successfully. It provides essential post-upgrade information to help users complete the upgrade process:

1. **Statistics Warning**: Informs users that optimizer statistics are not transferred during upgrade and recommends running vacuumdb with analyze-in-stages to regenerate them.

2. **User Specification Handling**: Constructs appropriate command-line options for vacuumdb, including the -U flag with the username if the user was explicitly specified during the upgrade.

3. **Cleanup Instructions**: Provides guidance on how to remove the old cluster's data files:
   - If a deletion script was successfully created, shows the path to run it
   - If no script could be created (due to user-defined tablespaces or directory conflicts), warns that manual deletion is required

The function uses PostgreSQL's PQExpBuffer for safe string construction and proper shell escaping.

## Parameters / Member Variables
- : Path to the script that can safely delete old cluster data files, or NULL if such a script couldn't be created due to directory conflicts or user-defined tablespaces

## Dependencies
- Functions called/Symbols referenced:
  -  (initializes string buffer)
  -  (appends string to buffer)
  -  (appends shell-escaped string)
  -  (appends single character)
  -  (cleanup buffer)
  -  (with PG_REPORT level)
  -  (global OS information)
  -  (username from OS info)
  -  (new cluster binary directory)
  -  (PostgreSQL string buffer type)
- Called from (representative examples):
  -  (in src/bin/pg_upgrade/pg_upgrade.c:234)

## Notes and Other Information
- This is the final user-facing output of a successful pg_upgrade run
- The vacuumdb recommendation is critical because pg_upgrade doesn't transfer optimizer statistics, which can severely impact query performance until regenerated
- The --analyze-in-stages option is recommended to reduce the initial load on the upgraded server
- Shell string escaping ensures that usernames containing special characters are handled safely
- The distinction between automatic and manual cleanup helps prevent accidental data loss in complex configurations