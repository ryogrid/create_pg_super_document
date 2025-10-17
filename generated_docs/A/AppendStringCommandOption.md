# AppendStringCommandOption

## Location
[src/bin/pg_basebackup/streamutil.c:833-855](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/streamutil.c#L833-L855)

## Overview
A utility function that appends a command option with an associated string value to a PostgreSQL server command buffer, with proper SQL string escaping for safety.

## Definition

```c
void
AppendStringCommandOption(PQExpBuffer buf, bool use_new_option_syntax,
						  char *option_name, char *option_value)
```
## Detailed Description
This function extends the functionality of AppendPlainCommandOption by adding support for string values that require proper SQL escaping. It first calls AppendPlainCommandOption to append the option name, then if a non-NULL option_value is provided, it escapes the string using PQescapeStringConn and appends it in single quotes to the command buffer. This ensures that special characters in the option value are properly handled and don't cause SQL injection vulnerabilities or parsing errors.

## Parameters / Member Variables
- `buf`: PQExpBuffer to append the option to
- `use_new_option_syntax`: Boolean flag indicating whether to use new or legacy option syntax
- `*option_name`: Name of the command option to append
- `*option_value`: String value for the option (may be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - [AppendPlainCommandOption](AppendPlainCommandOption.md)
  - [PQescapeStringConn](../P/PQescapeStringConn.md)
- Called from (representative examples):
  - [BaseBackup](../B/BaseBackup.md) (multiple calls in pg_basebackup.c)
  - [CreateReplicationSlot](../C/CreateReplicationSlot.md)

## Notes and Other Information
- The function safely handles NULL option values by checking before processing
- Uses PostgreSQL's built-in string escaping mechanism (PQescapeStringConn) to prevent SQL injection
- Memory management is handled properly with palloc/pfree for the escaped string buffer
- Part of the pg_basebackup utility's command construction infrastructure

## Simplified Source

```c
void AppendStringCommandOption(PQExpBuffer buf, bool use_new_option_syntax,
                              char *option_name, char *option_value) {
    // First append the option name using existing function
    AppendPlainCommandOption(buf, use_new_option_syntax, option_name);

    // If option has a value, escape and append it
    if (option_value != NULL) {
        size_t length = strlen(option_value);
        char *escaped_value = palloc(1 + 2 * length);

        // Escape the string value for safe SQL inclusion
        PQescapeStringConn(conn, escaped_value, option_value, length, NULL);

        // Append the escaped value in single quotes
        appendPQExpBuffer(buf, " '%s'", escaped_value);

        pfree(escaped_value);
    }
}
```