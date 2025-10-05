# BufFileTruncateFileSet

## Location
[src/backend/storage/file/buffile.c:933-1021](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/buffile.c#L933-L1021)

## Overview
Truncates a fileset-based BufFile to a specified file number and offset, removing excess segment files and adjusting the current buffer state accordingly.

## Definition
```c
void BufFileTruncateFileSet(BufFile *file, int fileno, off_t offset)
```

## Detailed Description
BufFileTruncateFileSet performs a comprehensive truncation operation on a fileset-based BufFile by removing all segment files beyond the specified file number and truncating the target file to the specified offset. The function handles several cases: it deletes files with numbers greater than fileno, removes the target file if offset is 0 (except for file 0), and truncates the target file to the specified offset. After physical file operations, it intelligently adjusts the current buffer state based on where the truncation point falls relative to the current buffer position. This ensures the BufFile remains in a consistent state after truncation.

## Parameters / Member Variables
- `file`: Pointer to the BufFile structure to truncate (must be fileset-based)
- `fileno`: The target file number within the fileset where truncation should occur
- `offset`: The byte offset within the target file where truncation should occur

## Dependencies
- Functions called/Symbols referenced:
  - [FileSetSegmentName](../F/FileSetSegmentName.md)
  - [FileClose](../F/FileClose.md)
  - [FileSetDelete](../F/FileSetDelete.md)
  - [FileTruncate](../F/FileTruncate.md)
  - [FilePathName](../F/FilePathName.md)
  - MAX_PHYSICAL_FILESIZE (constant)
  - WAIT_EVENT_BUFFILE_TRUNCATE
  - ereport/ERROR
- Called from (representative examples):
  - [stream_abort_internal](../s/stream_abort_internal.md) (src/backend/replication/logical/worker.c:1795)

## Notes and Other Information
- Only works with fileset-based BufFiles created by BufFileCreateFileSet
- Removes files with numbers greater than the specified fileno
- If offset is 0, removes the target file unless it is file 0 (which gets truncated instead)
- Intelligently adjusts current buffer position and size based on truncation point location
- Reports errors via ereport() if file operations fail
- Used primarily by logical replication worker processes for stream abort operations
- The function maintains buffer consistency by handling three cases: truncation within current buffer, truncation before current position in same file, and truncation in earlier files
- Physical file operations are performed before buffer state adjustments to ensure atomicity

## Simplified Source

```c
void BufFileTruncateFileSet(BufFile *file, int fileno, off_t offset) {
    int numFiles = file->numFiles;
    int newFile = fileno;
    off_t newOffset = file->curOffset;
    char segment_name[MAXPGPATH];

    // Remove files beyond target fileno and truncate target file
    for (int i = file->numFiles - 1; i >= fileno; i--) {
        if ((i != fileno || offset == 0) && i != 0) {
            // Delete this segment file
            FileSetSegmentName(segment_name, file->name, i);
            FileClose(file->files[i]);
            if (!FileSetDelete(file->fileset, segment_name, true))
                ereport(ERROR, /* deletion error */);
            numFiles--;
            newOffset = MAX_PHYSICAL_FILESIZE;
            if (i == fileno)
                newFile--;
        } else {
            // Truncate this file to the specified offset
            if (FileTruncate(file->files[i], offset, WAIT_EVENT_BUFFILE_TRUNCATE) < 0)
                ereport(ERROR, /* truncation error */);
            newOffset = offset;
        }
    }

    file->numFiles = numFiles;

    // Adjust buffer state based on truncation point location
    if (newFile == file->curFile &&
        newOffset >= file->curOffset &&
        newOffset <= file->curOffset + file->nbytes) {
        // Truncation within current buffer - adjust position and size
        if (newOffset <= file->curOffset + file->pos)
            file->pos = (int)(newOffset - file->curOffset);
        file->nbytes = (int)(newOffset - file->curOffset);
    } else if (newFile == file->curFile && newOffset < file->curOffset) {
        // Truncation before current position - reset buffer
        file->curOffset = newOffset;
        file->pos = 0;
        file->nbytes = 0;
    } else if (newFile < file->curFile) {
        // Truncation in earlier file - reset to new position
        file->curFile = newFile;
        file->curOffset = newOffset;
        file->pos = 0;
        file->nbytes = 0;
    }
    // No action needed if truncation is beyond current position
}
```