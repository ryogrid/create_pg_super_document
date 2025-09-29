# BufFileSeek

## Location
[src/backend/storage/file/buffile.c:740-832](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/buffile.c#L740-L832)

## Overview
Positions the file pointer within a buffered file, supporting both single-file and multi-segment file operations with large file size handling.

## Definition
```c
int BufFileSeek(BufFile *file, int fileno, off_t offset, int whence)
```

## Detailed Description
BufFileSeek provides file positioning functionality similar to fseek(), but with enhanced support for large files that exceed the maximum value representable by off_t. It handles multi-segment files where each segment can be up to 1GB (MAX_PHYSICAL_FILESIZE = 0x40000000 bytes), allowing BufFiles to span multiple segments and tablespaces.

The function supports three positioning modes:
- SEEK_SET: Absolute positioning using fileno and offset
- SEEK_CUR: Relative positioning from current location (ignores fileno, uses only offset)
- SEEK_END: Positioning relative to end of file

The function optimizes for seeks within the current buffer by simply adjusting the position pointer without flushing or reloading data. For seeks outside the current buffer, it flushes any dirty data and repositions the file to load the appropriate segment.

## Parameters / Member Variables
- `file`: Pointer to the BufFile structure to seek within
- `fileno`: File segment number for absolute seeks (ignored for relative seeks)
- `offset`: Byte offset for positioning within the file/segment
- `whence`: Positioning mode (SEEK_SET, SEEK_CUR, or SEEK_END)

## Dependencies
- Functions called/Symbols referenced:
  - [FileSize](../F/FileSize.md) (to determine file size for SEEK_END operations)
  - [FilePathName](../F/FilePathName.md) (for error reporting)
  - [BufFileFlush](BufFileFlush.md) (to ensure buffer consistency when repositioning)
  - MAX_PHYSICAL_FILESIZE (constant defining segment size limit)
- Called from (representative examples):
  - [SendBackupManifest](../S/SendBackupManifest.md) (backup operations seeking within manifest)
  - [ExecHashJoinNewBatch](../E/ExecHashJoinNewBatch.md) (hash join batch processing)
  - [ensure_last_message](../e/ensure_last_message.md) (logical replication message handling)
  - [stream_open_file](../s/stream_open_file.md) (logical replication streaming)
  - [BufFileSeekBlock](BufFileSeekBlock.md) (block-level seeking wrapper)
  - [tuplestore_select_read_pointer](../t/tuplestore_select_read_pointer.md) (tuplestore pointer selection)
  - [tuplestore_puttuple_common](../t/tuplestore_puttuple_common.md) (tuplestore writing operations)
  - [tuplestore_gettuple](../t/tuplestore_gettuple.md) (tuplestore reading operations)
  - [tuplestore_rescan](../t/tuplestore_rescan.md) (tuplestore rescanning)
  - [tuplestore_copy_read_pointer](../t/tuplestore_copy_read_pointer.md) (tuplestore pointer copying)

## Notes and Other Information
- Returns 0 on success, EOF on failure (without moving the logical position)
- Supports files larger than off_t limits by using multiple file segments
- Each segment is limited to 1GB (MAX_PHYSICAL_FILESIZE) to spread large files across tablespaces
- Optimizes seeks within the current buffer by avoiding I/O operations
- Automatically handles segment boundary crossings during positioning
- Flushes dirty buffer data only when necessary (when seeking outside current buffer)
- Uses PostgreSQL's error reporting via ereport() for I/O errors
- The file position is represented as (curFile, curOffset + pos) internally
- Supports negative offsets in relative seeks, with proper segment boundary handling
- SEEK_END operations determine file size by checking the last segment
- Invalid whence values trigger an elog(ERROR) and return EOF

## Simplified Source

```c
int
BufFileSeek(BufFile *file, int fileno, off_t offset, int whence)
{
    int     newFile;
    off_t   newOffset;

    switch (whence)
    {
        case SEEK_SET:
            if (fileno < 0)
                return EOF;
            newFile = fileno;
            newOffset = offset;
            break;
        case SEEK_CUR:
            newFile = file->curFile;
            newOffset = (file->curOffset + file->pos) + offset;
            break;
        case SEEK_END:
            newFile = file->numFiles - 1;
            newOffset = FileSize(file->files[file->numFiles - 1]);
            if (newOffset < 0)
                ereport(ERROR,
                        (errcode_for_file_access(),
                         errmsg("could not determine size of temporary file \"%s\" from BufFile \"%s\": %m",
                                FilePathName(file->files[file->numFiles - 1]),
                                file->name)));
            break;
        default:
            elog(ERROR, "invalid whence: %d", whence);
            return EOF;
    }

    while (newOffset < 0)
    {
        if (--newFile < 0)
            return EOF;
        newOffset += MAX_PHYSICAL_FILESIZE;
    }

    if (newFile == file->curFile &&
        newOffset >= file->curOffset &&
        newOffset <= file->curOffset + file->nbytes)
    {
        file->pos = (int) (newOffset - file->curOffset);
        return 0;
    }

    BufFileFlush(file);

    if (newFile == file->numFiles && newOffset == 0)
    {
        newFile--;
        newOffset = MAX_PHYSICAL_FILESIZE;
    }
    while (newOffset > MAX_PHYSICAL_FILESIZE)
    {
        if (++newFile >= file->numFiles)
            return EOF;
        newOffset -= MAX_PHYSICAL_FILESIZE;
    }
    if (newFile >= file->numFiles)
        return EOF;

    file->curFile = newFile;
    file->curOffset = newOffset;
    file->pos = 0;
    file->nbytes = 0;
    return 0;
}
```