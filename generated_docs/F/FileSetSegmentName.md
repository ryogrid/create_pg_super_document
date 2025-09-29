# FileSetSegmentName

## Location
[src/backend/storage/file/buffile.c:222-230](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/buffile.c#L222-L230)

## Overview
Constructs the filename for a specific segment of a named BufFile within a FileSet by appending a segment number to the base filename.

## Definition
```c
static void FileSetSegmentName(char *name, const char *buffile_name, int segment)
```

## Detailed Description
FileSetSegmentName is a utility function that generates standardized filenames for individual segments of multi-file BufFiles managed within a FileSet. It follows a simple naming convention by appending a dot and segment number to the base filename (e.g., "myfile.0", "myfile.1", etc.). This consistent naming scheme allows the system to manage multiple file segments as parts of a single logical BufFile.

The function uses snprintf to safely format the filename, ensuring it does not exceed MAXPGPATH characters. This naming convention is used throughout the FileSet system for creating, opening, and deleting file segments.

## Parameters / Member Variables
- `name`: Output buffer to store the generated segment filename (must be at least MAXPGPATH characters)
- `buffile_name`: Base name of the BufFile (without segment suffix)
- `segment`: Segment number to append to the filename (typically starts from 0)

## Dependencies
- Functions called/Symbols referenced:
  - snprintf (standard library function for safe string formatting)
  - MAXPGPATH (PostgreSQL constant defining maximum path length)
- Called from (representative examples):
  - [MakeNewFileSetSegment](../M/MakeNewFileSetSegment.md) (when creating new file segments)
  - [BufFileOpenFileSet](../B/BufFileOpenFileSet.md) (when opening existing file segments)
  - [BufFileDeleteFileSet](../B/BufFileDeleteFileSet.md) (when deleting file segments)
  - [BufFileTruncateFileSet](../B/BufFileTruncateFileSet.md) (when truncating file segments)

## Notes and Other Information
- This is a static function internal to buffile.c, not exposed to external modules
- The naming convention follows the pattern "basename.segmentnum" (e.g., "data.0", "data.1")
- Segment numbering typically starts from 0 for the first file
- The function assumes the output buffer is at least MAXPGPATH characters long
- Used exclusively in the context of FileSet-managed BufFiles, not standalone temporary files
- The generated names are used consistently across FileSet operations for segment management

## Simplified Source

```c
static void
FileSetSegmentName(char *name, const char *buffile_name, int segment)
{
    // Create segment filename: "basename.segmentnum"
    snprintf(name, MAXPGPATH, "%s.%d", buffile_name, segment);
}
```