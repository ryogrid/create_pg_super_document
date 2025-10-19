# read_connect_arg

## Location
[src/bin/psql/command.c:3103-3142](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L3103-L3142)

## Overview
Parses and extracts connection arguments from psql `\connect` command input, handling special cases for backwards compatibility and SQL identifier quoting rules.

## Definition
```c
static char * read_connect_arg(PsqlScanState scan_state)
```

## Detailed Description
This function reads and interprets arguments passed to the `\connect` psql meta-command. It implements special parsing logic that balances SQL identifier standards with backwards compatibility requirements from older PostgreSQL versions. The function uses the OT_SQLIDHACK option type to handle mixed-case identifiers properly while maintaining compatibility with pg_dump files from PostgreSQL 7.2 and earlier.

The function treats empty arguments and single dash ("-") arguments as NULL, which typically indicates using default connection parameters. Quoted arguments are handled according to SQL identifier rules, while unquoted arguments are preserved verbatim for backwards compatibility.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer containing the current parsing state and input buffer for extracting connection arguments

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option (to extract the connection argument with special SQL identifier handling)
  - strcmp (to check for special "-" argument)
  - free (to deallocate memory when returning NULL for empty/dash arguments)
- Called from (representative examples):
  - [exec_command_connect](../e/exec_command_connect.md) (multiple times to read database, user, host, and port arguments)

## Notes and Other Information
- Returns a mallocd string containing the connection argument, or NULL for no/empty arguments
- Uses OT_SQLIDHACK for backwards compatibility with older pg_dump files that may not quote mixed-case identifiers
- Treats both empty strings and "-" as indicators to use default connection parameters
- The function includes extensive comments about the historical reasons for the current parsing behavior
- Part of the connection management system in psql, specifically handling argument parsing for database connections
- Memory management: caller is responsible for freeing the returned string when non-NULL

## Simplified Source

```c
static char *
read_connect_arg(PsqlScanState scan_state)
{
    char *result;
    char quote;

    // Parse connection argument with SQL identifier handling
    // (OT_SQLIDHACK for backwards compatibility with old pg_dump files)
    result = psql_scan_slash_option(scan_state, OT_SQLIDHACK, &quote, true);

    if (!result)
        return NULL;

    // If quoted, return as-is
    if (quote)
        return result;

    // Handle special cases: empty string or "-" means use default
    if (*result == '\0' || strcmp(result, "-") == 0) {
        free(result);
        return NULL;
    }

    return result;
}
```