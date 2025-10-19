# appendConnStrVal

## Location
[src/fe_utils/string_utils.c:698-742](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/string_utils.c#L698-L742)

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
  - [appendPQExpBufferChar](appendPQExpBufferChar.md) (appends individual characters including quotes and escapes)
  - [appendPQExpBufferStr](appendPQExpBufferStr.md) (appends the raw string when no quoting is needed)
- Called from (representative examples):
  - [constructConnStr](../c/constructConnStr.md) (in pg_dumpall.c for building connection strings)
  - [get_db_conn](../g/get_db_conn.md) (in pg_upgrade for database connections)
  - [GenerateRecoveryConfig](../G/GenerateRecoveryConfig.md) (in recovery_gen.c for recovery configuration)
  - [appendPsqlMetaConnect](appendPsqlMetaConnect.md) (in string_utils.c for psql meta-commands)

## Notes and Other Information
- Uses conservative quoting rules - only alphanumeric characters, underscore, and period are considered safe
- Implements proper escaping for single quotes (\') and backslashes (\\) within quoted values
- Essential for secure construction of PostgreSQL connection strings in utilities
- Follows libpq connection string syntax requirements exactly
- Used extensively in PostgreSQL utilities that need to construct connection strings dynamically
- The conservative approach to quoting ensures compatibility across different PostgreSQL versions and environments
- Does not validate the input string format - assumes valid C string input

## Simplified Source

```c
void appendConnStrVal(PQExpBuffer buf, const char *str) {
    const char *s;
    bool needquotes;

    // Check if string contains only safe characters (letters, digits, _, .)
    needquotes = true;
    for (s = str; *s; s++) {
        if (!((*s >= 'a' && *s <= 'z') || (*s >= 'A' && *s <= 'Z') ||
              (*s >= '0' && *s <= '9') || *s == '_' || *s == '.')) {
            needquotes = true;
            break;
        }
        needquotes = false;  // All characters so far are safe
    }

    if (needquotes) {
        // Quote and escape the string
        appendPQExpBufferChar(buf, '\'');
        while (*str) {
            // Escape single quotes and backslashes by doubling them
            if (*str == '\'' || *str == '\\')
                appendPQExpBufferChar(buf, '\\');

            appendPQExpBufferChar(buf, *str);
            str++;
        }
        appendPQExpBufferChar(buf, '\'');
    } else {
        // Safe string - no quoting needed
        appendPQExpBufferStr(buf, str);
    }
}
```