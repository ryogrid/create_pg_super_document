# appendConnStrVal

## Location
src/fe_utils/string_utils.c: 698 - 742

## Overview
Appends a string value to a PostgreSQL libpq connection string with appropriate quoting and escaping for keyword/value pair syntax.

## Definition
```c
void appendConnStrVal(PQExpBuffer buf, const char *str)
```

## Detailed Description
This function formats string values for inclusion in PostgreSQL connection strings (conninfo strings) according to libpq requirements. It implements a conservative approach to determine when quoting is necessary - only strings containing exclusively safe ASCII characters (letters, digits, underscore, and period) are left unquoted.

When quoting is required, the function wraps the string in single quotes and escapes any embedded single quotes or backslashes by doubling them (\' becomes \\', \\ becomes \\\\). This follows the standard PostgreSQL connection string value quoting rules.

The function is essential for building dynamic connection strings safely, ensuring that special characters in values don't break the connection string parsing or introduce security vulnerabilities.

## Parameters / Member Variables
- `buf`: Target PQExpBuffer where the formatted connection string value will be appended
- `str`: Input string value to be formatted and appended

## Dependencies
- Functions called/Symbols referenced:
  - appendPQExpBufferChar (appends individual characters including quotes and escapes)
  - appendPQExpBufferStr (appends the raw string when no quoting is needed)
- Called from (representative examples):
  - constructConnStr (in pg_dumpall.c for building connection strings)
  - get_db_conn (in pg_upgrade for database connections)
  - GenerateRecoveryConfig (in recovery_gen.c for recovery configuration)
  - appendPsqlMetaConnect (in string_utils.c for psql meta-commands)

## Notes and Other Information
- Uses conservative quoting rules - only alphanumeric characters, underscore, and period are considered safe
- Implements proper escaping for single quotes (\') and backslashes (\\) within quoted values
- Essential for secure construction of PostgreSQL connection strings in utilities
- Follows libpq connection string syntax requirements exactly
- Used extensively in PostgreSQL utilities that need to construct connection strings dynamically
- The conservative approach to quoting ensures compatibility across different PostgreSQL versions and environments
- Does not validate the input string format - assumes valid C string input