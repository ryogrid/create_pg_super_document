# BufFileDumpBuffer

## Location
[src/backend/storage/file/buffile.c:494-592](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/buffile.c#L494-L592)

## Overview
BufFileDumpBuffer writes the contents of a BufFile's dirty buffer to the underlying file system, handling multi-file scenarios and maintaining logical file positioning.

## Definition

```c
static void
BufFileDumpBuffer(BufFile *file)
```
## Detailed Description
BufFileDumpBuffer is an internal function that performs the critical task of flushing dirty buffer contents to persistent storage. Unlike BufFileLoadBuffer, this function must handle the entire buffer content even if it spans multiple component files, requiring a loop-based approach to ensure all data is written.

The function manages several complex scenarios:
1. **Multi-file writes**: When buffer contents exceed the remaining space in the current file, it automatically creates new component files and continues writing
2. **File size limits**: Ensures no single component file exceeds MAX_PHYSICAL_FILESIZE by splitting writes across files
3. **Position management**: Carefully maintains both physical file offset and logical buffer position after the write
4. **Backwards seek handling**: Properly adjusts file position when the logical position is less than the written data end

Key operations performed:
- Loops through buffer contents, writing chunks that fit within file size limits
- Extends the file set by creating new component files as needed
- Tracks I/O timing and buffer usage statistics
- Handles error conditions with appropriate error reporting
- Maintains buffer state by clearing dirty flag and adjusting positions

## Parameters / Member Variables
- : Pointer to the BufFile structure with dirty buffer contents to be written

## Dependencies
- Functions called/Symbols referenced:
  - [extendBufFile](../e/extendBufFile.md) (creates new component files when needed)
  - [FileWrite](../F/FileWrite.md) (performs actual file write operations)
  - [FilePathName](../F/FilePathName.md) (gets file path for error reporting)
  - INSTR_TIME_SET_CURRENT (timing measurement)
  - INSTR_TIME_SET_ZERO (timing initialization)
  - INSTR_TIME_ACCUM_DIFF (timing accumulation)
  - ereport (error reporting)
- Called from (representative examples):
  - [BufFileWrite](BufFileWrite.md) (when buffer needs flushing during write operations)
  - [BufFileFlush](BufFileFlush.md) (explicit buffer flush requests)

## Notes and Other Information
- This is a static (internal) function, not part of the public BufFile API
- Assumes buffer is dirty (dirty = true) and contains data (nbytes > 0) on entry
- Must write the entire buffer contents, even across file boundaries, unlike read operations
- Automatically extends the file set when current files are insufficient
- Includes comprehensive I/O timing tracking for performance monitoring
- Updates global pgBufferUsage statistics for temporary block writes
- Handles complex position arithmetic to maintain logical file positioning after writes
- The function clears the dirty flag only after successful completion of all writes
- Critical for maintaining data integrity in buffered file operations
- Handles edge cases like backwards seeks in dirty buffers by proper offset calculations

## Simplified Source

```c
static void
BufFileDumpBuffer(BufFile *file)
{
    int wpos = 0;
    int bytestowrite;
    File thisfile;

    // Unlike loading, must dump whole buffer even across file boundaries
    while (wpos < file->nbytes)
    {
        off_t availbytes;
        instr_time io_start, io_time;

        // Create new file if current file is at size limit
        if (file->curOffset >= MAX_PHYSICAL_FILESIZE)
        {
            while (file->curFile + 1 >= file->numFiles)
                extendBufFile(file);
            file->curFile++;
            file->curOffset = 0;
        }

        // Calculate how much to write to this file
        bytestowrite = file->nbytes - wpos;
        availbytes = MAX_PHYSICAL_FILESIZE - file->curOffset;

        if ((off_t) bytestowrite > availbytes)
            bytestowrite = (int) availbytes;

        thisfile = file->files[file->curFile];

        // Track I/O timing if enabled
        if (track_io_timing)
            INSTR_TIME_SET_CURRENT(io_start);
        else
            INSTR_TIME_SET_ZERO(io_start);

        // Write data to file
        bytestowrite = FileWrite(thisfile,
                               file->buffer.data + wpos,
                               bytestowrite,
                               file->curOffset,
                               WAIT_EVENT_BUFFILE_WRITE);
        if (bytestowrite <= 0)
            ereport(ERROR,
                   (errcode_for_file_access(),
                    errmsg("could not write to file \"%s\": %m",
                           FilePathName(thisfile))));

        // Update I/O timing statistics
        if (track_io_timing)
        {
            INSTR_TIME_SET_CURRENT(io_time);
            INSTR_TIME_ACCUM_DIFF(pgBufferUsage.temp_blk_write_time, io_time, io_start);
        }

        // Advance positions
        file->curOffset += bytestowrite;
        wpos += bytestowrite;

        pgBufferUsage.temp_blks_written++;
    }
    file->dirty = false;

    // Adjust position for logical file positioning
    // (handle case where logical position < written data end)
    file->curOffset -= (file->nbytes - file->pos);
    if (file->curOffset < 0)    // handle segment crossing
    {
        file->curFile--;
        Assert(file->curFile >= 0);
        file->curOffset += MAX_PHYSICAL_FILESIZE;
    }

    // Reset buffer to empty state
    file->pos = 0;
    file->nbytes = 0;
}
```