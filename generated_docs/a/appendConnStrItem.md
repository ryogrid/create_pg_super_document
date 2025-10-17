# appendConnStrItem

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:249-270](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L249-L270)

## Overview
Helper function that appends a keyword-value pair to a PostgreSQL connection string buffer with proper formatting and quoting.

## Definition

```c
static void
appendConnStrItem(PQExpBuffer buf, const char *keyword, const char *val)
```
## Detailed Description
This utility function constructs PostgreSQL connection strings by appending individual keyword-value pairs to a PQExpBuffer. It handles the proper formatting of connection string items by:

1. Adding a space separator if the buffer already contains content
2. Appending the keyword name directly (assuming keywords don't need quoting)
3. Adding an equals sign separator
4. Appending the value with proper quoting/escaping via appendConnStrVal()

This function is essential for building connection strings dynamically in pg_createsubscriber, ensuring that connection parameters are properly formatted according to PostgreSQL's connection string syntax.

## Parameters / Member Variables
- `buf`: PQExpBuffer to append the formatted keyword-value pair to
- `*keyword`: Connection string parameter name (e.g., "host", "port", "dbname")
- `*val`: Value to be associated with the keyword, will be properly quoted/escaped
## Dependencies
- Functions called/Symbols referenced:
  - [appendPQExpBufferChar](appendPQExpBufferChar.md) (to add space and equals sign characters)
  - [appendPQExpBufferStr](appendPQExpBufferStr.md) (to add the keyword string)
  - [appendConnStrVal](appendConnStrVal.md) (to add the properly quoted/escaped value)
- Called from (representative examples):
  - [get_base_conninfo](../g/get_base_conninfo.md)
  - [get_sub_conninfo](../g/get_sub_conninfo.md)
  - [concat_conninfo_dbname](../c/concat_conninfo_dbname.md)

## Notes and Other Information
- This is a static function specific to pg_createsubscriber utility
- Assumes that keywords in connection strings don't require quoting or escaping
- Relies on appendConnStrVal() to handle proper quoting and escaping of values
- Used extensively in building connection strings for both primary and subscriber database connections
- Part of the connection string building infrastructure in pg_createsubscriber
- Maintains proper connection string format with space-separated keyword=value pairs

## Simplified Source

```c
static void appendConnStrItem(PQExpBuffer buf, const char *keyword, const char *val) {
    // Add space separator if buffer has existing content
    if (buf->len > 0)
        appendPQExpBufferChar(buf, ' ');

    // Append keyword=value with proper value quoting
    appendPQExpBufferStr(buf, keyword);
    appendPQExpBufferChar(buf, '=');
    appendConnStrVal(buf, val);
}
```