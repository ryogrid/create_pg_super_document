# BufFileExportFileSet

## Location
[src/backend/storage/file/buffile.c:394-411](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/buffile.c#L394-L411)

## Overview
Flushes a fileset-based BufFile and marks it as read-only in preparation for sharing with other backends.

## Definition

```c
void
BufFileExportFileSet(BufFile *file)
```
## Detailed Description
BufFileExportFileSet prepares a fileset-based BufFile for sharing with other backends by ensuring all data is flushed to disk and marking the file as read-only. This function is essential for the inter-backend file sharing mechanism, as it ensures data integrity and prevents further modifications by the creating backend.

The function performs two critical operations: first, it flushes any buffered data to ensure all writes are persisted to disk, then it sets the readOnly flag to prevent any further write operations. This guarantees that other backends opening the same BufFile will see a consistent, complete dataset.

## Parameters / Member Variables
- `*file`: Pointer to the BufFile to export (must be backed by a FileSet)
## Dependencies
- Functions called/Symbols referenced:
  - Assert: Validates that the file belongs to a FileSet and is not already read-only
  - [BufFileFlush](BufFileFlush.md): Ensures all buffered data is written to disk before marking as read-only
- Called from (representative examples):
  - [LogicalTapeFreeze](../L/LogicalTapeFreeze.md): When freezing logical tapes for sharing in sort operations

## Notes and Other Information
- The BufFile must belong to a FileSet (file->fileset != NULL) - enforced by assertion
- The function expects to be called only once per BufFile (file->readOnly must be false)
- Essential for the inter-backend sharing protocol - other backends should only open exported BufFiles
- After export, the BufFile becomes read-only and cannot be written to further
- Calling this function twice on the same BufFile is considered a programming error
- Alternative to BufFileClose() when the creating backend wants to keep the BufFile open while allowing others to read it
- Critical for ensuring data consistency in PostgreSQL's shared temporary file system

## Simplified Source

```c
void BufFileExportFileSet(BufFile *file) {
    // Must be a fileset-based file that hasn't been exported yet
    Assert(file->fileset != NULL);
    Assert(!file->readOnly);

    // Flush all buffered data to disk for sharing
    BufFileFlush(file);

    // Mark as read-only to prevent further writes
    file->readOnly = true;
}
```