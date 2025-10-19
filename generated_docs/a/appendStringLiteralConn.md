# appendStringLiteralConn

## Location
[src/fe_utils/string_utils.c:446-483](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/string_utils.c#L446-L483)

## Overview
Converts a string value to a properly escaped SQL string literal using connection-specific encoding and syntax rules, with special handling for escape sequences.

## Definition
```c
void appendStringLiteralConn(PQExpBuffer buf, const char *str, PGconn *conn)
```

## Detailed Description
The `appendStringLiteralConn` function creates SQL string literals using the encoding and standard_conforming_strings settings from an active database connection. This is the preferred method when a PGconn is available, as it automatically uses the connection's configuration rather than requiring explicit parameter specification.

The function includes special handling for strings containing backslashes on PostgreSQL 8.1 and later servers. When backslashes are detected, it uses the escape string syntax (E'...') to silence escape_string_warning messages in utility programs. This is implemented as a temporary workaround (marked as XXX kluge) that may be removed in future versions.

For strings without special escape sequences, the function delegates to libpq's `PQescapeStringConn` for optimal performance and consistency with the established PostgreSQL escaping mechanisms.

## Parameters / Member Variables
- `buf`: Output PQExpBuffer to append the escaped string literal to
- `str`: Input string to be converted to SQL literal format  
- `conn`: Active PostgreSQL connection providing encoding and syntax settings

## Dependencies
- Functions called/Symbols referenced:
  - [PQserverVersion](../P/PQserverVersion.md) (check server version for escape syntax support)
  - `[appendPQExpBufferChar](appendPQExpBufferChar.md)` (buffer character append operations)
  - `ESCAPE_STRING_SYNTAX` (escape string marker constant)
  - [appendStringLiteral](appendStringLiteral.md) (fallback for escape string cases)
  - [PQclientEncoding](../P/PQclientEncoding.md) (get connection encoding)
  - [enlargePQExpBuffer](../e/enlargePQExpBuffer.md) (buffer space management)
  - [PQescapeStringConn](../P/PQescapeStringConn.md) (libpq string escaping)
- Called from (representative examples):
  - [append_db_pattern_cte](append_db_pattern_cte.md) (src/bin/pg_amcheck/pg_amcheck.c:1557)
  - [emitShSecLabels](../e/emitShSecLabels.md) (src/bin/pg_dump/dumputils.c:710)
  - [dumpRoles](../d/dumpRoles.md) (src/bin/pg_dump/pg_dumpall.c:945, 957)
  - [create_logical_replication_slots](../c/create_logical_replication_slots.md) (src/bin/pg_upgrade/pg_upgrade.c:957, 959)

## Notes and Other Information
- Preferred over `appendStringLiteral` when database connection is available
- Contains temporary workaround code for escape_string_warning suppression
- Automatically handles connection-specific encoding and standards compliance
- Uses efficient `PQescapeStringConn` for most common cases
- Special escape string syntax handling for PostgreSQL 8.1+ servers
- Widely used throughout PostgreSQL client utilities for safe SQL generation
- Ensures proper spacing when using escape string syntax to avoid identifier adjacency issues

## Simplified Source

```c
void appendStringLiteralConn(PQExpBuffer buf, const char *str, PGconn *conn) {
    size_t length = strlen(str);

    // Special handling for strings with backslashes on newer servers
    if (strchr(str, '\\') != NULL && PQserverVersion(conn) >= 80100) {
        // Ensure space before escape string syntax to avoid identifier issues
        if (buf->len > 0 && buf->data[buf->len - 1] != ' ')
            appendPQExpBufferChar(buf, ' ');

        // Use escape string syntax E'...'
        appendPQExpBufferChar(buf, ESCAPE_STRING_SYNTAX);
        appendStringLiteral(buf, str, PQclientEncoding(conn), false);
        return;
    }

    // Standard case: use libpq's connection-aware escaping
    if (!enlargePQExpBuffer(buf, 2 * length + 2))
        return;

    appendPQExpBufferChar(buf, '\'');
    buf->len += PQescapeStringConn(conn, buf->data + buf->len, str, length, NULL);
    appendPQExpBufferChar(buf, '\'');
}
```