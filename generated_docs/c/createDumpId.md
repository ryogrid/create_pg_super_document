# createDumpId

## Location
[src/bin/pg_dump/common.c:734-742](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/common.c#L734-L742)

## Overview
Generates a unique DumpId that is not tied to any DumpableObject, primarily used for creating fixed ArchiveEntry objects in pg_dump.

## Definition

```c
DumpId
createDumpId(void)
```
## Detailed Description
This function provides a simple mechanism to generate unique dump identifiers that are independent of any DumpableObject. It increments and returns the global lastDumpId counter. These standalone DumpIds are specifically designed for creating "fixed" ArchiveEntry objects that don't need to participate in pg_dump's dependency sorting logic, such as database-level settings, encoding information, and other metadata entries that have predetermined ordering requirements.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - lastDumpId (global variable that tracks the highest assigned DumpId)
- Data structures used:
  - DumpId (return type)
- Called from (representative examples):
  - [dumpDatabase](../d/dumpDatabase.md) (src/bin/pg_dump/pg_dump.c: multiple lines)
  - [dumpEncoding](../d/dumpEncoding.md) (src/bin/pg_dump/pg_dump.c:3576)
  - [dumpStdStrings](../d/dumpStdStrings.md) (src/bin/pg_dump/pg_dump.c:3601)
  - [dumpSearchPath](../d/dumpSearchPath.md) (src/bin/pg_dump/pg_dump.c:3655)
  - [dumpCommentExtended](../d/dumpCommentExtended.md) (src/bin/pg_dump/pg_dump.c:10225)
  - [dumpACL](../d/dumpACL.md) (src/bin/pg_dump/pg_dump.c:15351)
  - [dumpSecLabel](../d/dumpSecLabel.md) (src/bin/pg_dump/pg_dump.c:15448)
  - [dumpSequence](../d/dumpSequence.md) (src/bin/pg_dump/pg_dump.c:17808)

## Notes and Other Information
- Returns a unique DumpId by incrementing the global lastDumpId counter
- Used for ArchiveEntry objects that don't require dependency sorting
- Simple atomic operation that ensures uniqueness across the entire dump process
- Critical for maintaining proper ordering of fixed entries in the dump output
- The returned DumpId will never conflict with DumpIds assigned to DumpableObjects