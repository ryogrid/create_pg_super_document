# bbsink_copystream_end_manifest

## Location
[src/backend/backup/basebackup_copy.c:288-296](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_copy.c#L288-L296)

## Overview
A no-op function that signals the end of backup manifest transmission in a copystream-based backup sink.

## Definition
static void bbsink_copystream_end_manifest(bbsink *sink)

## Detailed Description
This function serves as a placeholder callback for ending manifest transmission in the bbsink copystream implementation. According to the comment, no explicit terminator is needed for the backup manifest in the copystream protocol, so the function intentionally does nothing. It exists to satisfy the bbsink interface requirements while providing a clear indication that manifest termination is handled implicitly by the protocol.

## Parameters / Member Variables
- `sink`: Pointer to the base bbsink structure representing the copystream backup sink (unused)

## Dependencies
- Functions called/Symbols referenced:
  - None (function body is empty)
- Called from (representative examples):
  - Used as callback function at the end of manifest transmission in bbsink copystream operations

## Notes and Other Information
- This is a static function internal to the basebackup_copy.c module
- Part of the bbsink copystream implementation for PostgreSQL base backups
- Intentionally does nothing as no explicit manifest terminator is required
- Exists to fulfill the bbsink interface contract for end_manifest operations
- Located in src/backend/backup/basebackup_copy.c:288-296

## Simplified Source

```c
static void bbsink_copystream_end_manifest(bbsink *sink)
{
    // No explicit terminator needed for backup manifest in copystream protocol
    /* Do nothing. */
}
```