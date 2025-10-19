# escape_quotes

## Location
[src/bin/scripts/vacuumdb.c:452-474](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/scripts/vacuumdb.c#L452-L474)

## Overview
A wrapper function that escapes single quotes and backslashes in strings to make them suitable for insertion into configuration files or SQL E-string literals.

## Definition
```c
static char *escape_quotes(const char *src)
```

## Detailed Description
This function provides a convenient interface for escaping special characters in strings that will be embedded in configuration files or SQL statements. It acts as a wrapper around the more general `escape_single_quotes_ascii()` function, adding error handling for out-of-memory conditions. The function ensures that single quotes and backslashes are properly escaped to prevent syntax errors or security issues when the resulting string is used in SQL contexts or configuration file values.

The function is particularly useful during database initialization (initdb) when generating configuration files and initial SQL scripts that may contain user-provided values or system-detected values that could contain special characters.

## Parameters / Member Variables
- `src`: The source string to be escaped (const char *)

## Dependencies
- Functions called/Symbols referenced:
  - [escape_single_quotes_ascii](escape_single_quotes_ascii.md) (performs the actual character escaping)
  - [pg_fatal](../p/pg_fatal.md) (error reporting for out-of-memory conditions)
- Called from (representative examples):
  - [escape_quotes_bki](escape_quotes_bki.md) (BKI file escaping)
  - [replace_guc_value](../r/replace_guc_value.md) (GUC parameter value escaping)
  - [setup_auth](../s/setup_auth.md) (authentication configuration setup)
  - [setup_privileges](../s/setup_privileges.md) (privilege configuration setup)
  - [setup_schema](../s/setup_schema.md) (schema setup operations)
  - [GenerateRecoveryConfig](../G/GenerateRecoveryConfig.md) (recovery configuration generation)

## Notes and Other Information
- Returns a newly allocated string that must be freed by the caller
- Terminates the program with pg_fatal() if memory allocation fails
- Part of the initdb utilitys string processing infrastructure
- The `static` keyword indicates this function has internal linkage within initdb.c
- Designed specifically for PostgreSQL configuration and SQL contexts where E-string syntax is used

## Simplified Source

```c
static char *escape_quotes(const char *src) {
    // Escape single quotes and backslashes for safe SQL/config use
    char *result = escape_single_quotes_ascii(src);

    // Handle out of memory condition
    if (!result)
        pg_fatal("out of memory");

    return result;
}
```