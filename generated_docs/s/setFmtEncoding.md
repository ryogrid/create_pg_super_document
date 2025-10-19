# setFmtEncoding

## Location
[src/fe_utils/string_utils.c:69-77](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/string_utils.c#L69-L77)

## Overview
Sets the character encoding that fmtId() and fmtQualifiedId() functions will use for proper identifier escaping and quoting operations.

## Definition
```c
void setFmtEncoding(int encoding)
```

## Detailed Description
This function configures the global encoding setting used by PostgreSQL's identifier formatting functions. It sets the static variable fmtIdEncoding which is later used by fmtId() and fmtQualifiedId() to ensure proper character escaping when quoting database identifiers.

The function addresses the need for encoding-aware identifier processing in PostgreSQL frontend utilities. Different character encodings may require different escaping rules, and this function allows the formatting system to adapt to the connection's encoding.

However, the implementation has a known limitation: it is not safe for applications that maintain multiple database connections with different encodings simultaneously, as the encoding setting is global rather than per-connection. The comments indicate this is a recognized design issue that may be addressed in future versions.

## Parameters / Member Variables
- `encoding`: Integer value representing the character encoding (typically constants from PostgreSQL's encoding system like PG_UTF8, PG_LATIN1, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - fmtIdEncoding (static variable assignment)
- Called from (representative examples):
  - [processEncodingEntry](../p/processEncodingEntry.md) (pg_backup_archiver.c:2840)
  - [setup_connection](setup_connection.md) (pg_dump.c:1236)
  - [main](../m/main.md) (pg_dumpall.c:546)
  - [exec_command_encoding](../e/exec_command_encoding.md) (psql command.c:1360)
  - [SyncVariables](../S/SyncVariables.md) (psql command.c:4050)
  - [main](../m/main.md) (createdb.c:201)
  - [main](../m/main.md) (createuser.c:295)

## Notes and Other Information
- Sets the global fmtIdEncoding static variable which defaults to -1 (uninitialized)
- Used by pg_dump, pg_dumpall, psql, and other PostgreSQL frontend utilities
- Not thread-safe and not safe for multiple connections with different encodings
- The design may be deprecated in future versions as indicated by comments suggesting eventual removal of fmtId()
- Critical for proper handling of non-ASCII characters in database identifiers
- Should be called once per database connection to set the appropriate encoding for that session

## Simplified Source

```c
void setFmtEncoding(int encoding) {
    // Set global encoding for identifier formatting functions
    fmtIdEncoding = encoding;
}
```