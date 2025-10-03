# dumpAccessMethod

## Location
[src/bin/pg_dump/pg_dump.c:13274-13341](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L13274-L13341)

## Overview
Writes out a single access method definition to the pg_dump output, generating both CREATE ACCESS METHOD and DROP ACCESS METHOD statements.

## Definition

```c
static void
dumpAccessMethod(Archive *fout, const AccessMethodInfo *aminfo)
```
## Detailed Description
This function generates SQL statements to recreate an access method during database restoration. It constructs a CREATE ACCESS METHOD statement with the appropriate type (INDEX or TABLE) and handler function, along with a corresponding DROP statement for cleanup. The function handles binary upgrade scenarios and includes support for dumping associated comments.

The function validates the access method type and logs warnings for invalid types. It respects dump options such as data-only mode and component-specific dump flags. The generated statements are registered with the archive system for inclusion in the dump output.

## Parameters / Member Variables
- `*fout`: Archive handle for output generation and dump options
- `*aminfo`: AccessMethodInfo structure containing access method details including name, type, and handler
## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)/destroyPQExpBuffer (for SQL statement building)
  - [pg_strdup](../p/pg_strdup.md)/free (for memory management)
  - [fmtId](../f/fmtId.md) (for proper identifier formatting)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)/appendPQExpBufferStr (for statement construction)
  - pg_log_warning (for error logging)
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md) (for binary upgrade support)
  - [ArchiveEntry](../A/ArchiveEntry.md) (to register dump entry)
  - [dumpComment](dumpComment.md) (to handle access method comments)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md) (as part of general object dumping)
  - fmtQualifiedDumpable

## Notes and Other Information
- Skips execution in data-only dump mode
- Supports both INDEX and TABLE type access methods
- Validates access method type and handles invalid types gracefully
- Includes binary upgrade support for extension members
- Generates both creation and deletion statements
- Handles access method comments as separate dump components
- Uses proper SQL identifier formatting for access method names
- Part of PostgreSQL's pg_dump utility for database schema export
- Respects component-level dump flags for selective dumping