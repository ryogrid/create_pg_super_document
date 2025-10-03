# MakeNewFileSetSegment

## Location
[src/backend/storage/file/buffile.c:231-266](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/buffile.c#L231-L266)

## Overview
Creates a new segment file for a fileset-based BufFile, handling cleanup of any pre-existing segments to avoid confusion during recovery operations.

## Definition

```c
static File
MakeNewFileSetSegment(BufFile *buffile, int segment)
```
## Detailed Description
MakeNewFileSetSegment is a static function that creates a new file segment for a BufFile that is backed by a FileSet. The function is designed to handle crash recovery scenarios by proactively cleaning up any leftover files from previous executions that might confuse the system about the number of segments available.

The function first constructs the name for the next segment (segment + 1) and deletes it if it exists, ensuring that BufFileOpenFileSet() won't be confused about segment counts during recovery. It then creates the requested segment file using the FileSet infrastructure.

## Parameters / Member Variables
- `*buffile`: Pointer to the BufFile structure that needs a new segment
- `segment`: The segment number to create (0-based indexing)
## Dependencies
- Functions called/Symbols referenced:
  - [FileSetSegmentName](../F/FileSetSegmentName.md): Constructs standardized segment file names
  - [FileSetDelete](../F/FileSetDelete.md): Removes potentially conflicting segment files
  - [FileSetCreate](../F/FileSetCreate.md): Creates the new segment file in the fileset
  - Assert: Validates successful file creation
- Called from (representative examples):
  - [extendBufFile](../e/extendBufFile.md): When extending a BufFile with additional segments
  - [BufFileCreateFileSet](../B/BufFileCreateFileSet.md): During initial BufFile creation with filesets

## Notes and Other Information
- This is a static function, only accessible within buffile.c
- The function proactively deletes the next segment (segment + 1) to prevent confusion during crash recovery
- Returns a File handle that must be > 0 (validated by Assert)
- Part of PostgreSQL's temporary file management system for handling large data sets that don't fit in memory
- The segment cleanup strategy ensures consistent state after system crashes or restarts

## Simplified Source

```c
static File
MakeNewFileSetSegment(BufFile *buffile, int segment)
{
    char name[MAXPGPATH];
    File file;

    // Clean up any leftover segment from previous crashes
    // This prevents confusion about segment count during recovery
    FileSetSegmentName(name, buffile->name, segment + 1);
    FileSetDelete(buffile->fileset, name, true);

    // Create the new segment file
    FileSetSegmentName(name, buffile->name, segment);
    file = FileSetCreate(buffile->fileset, name);

    // Verify successful creation
    Assert(file > 0);

    return file;
}
```