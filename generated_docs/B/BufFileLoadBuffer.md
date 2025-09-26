# BufFileLoadBuffer

## Location
src/backend/storage/file/buffile.c: 434 - 493

## Overview
BufFileLoadBuffer loads data from the underlying file into the BufFile's internal buffer, handling multi-file scenarios and I/O timing tracking.

## Definition

```c
static void
BufFileLoadBuffer(BufFile *file)
```
## Detailed Description
BufFileLoadBuffer is an internal function responsible for reading data from the underlying file system into the BufFile's buffer. It handles the complexity of BufFiles that may span multiple physical files by automatically advancing to the next file when the current file reaches the maximum physical file size limit.

The function performs several key operations:
1. Checks if the current file has reached its size limit and advances to the next file if available
2. Reads data from the current file starting at the current offset position
3. Tracks I/O timing statistics if enabled for performance monitoring
4. Updates buffer usage statistics for temporary block reads
5. Handles read errors by reporting appropriate error messages

The function assumes that on entry, the buffer is clean (not dirty), and both position and nbytes are zero. On exit, nbytes contains the number of bytes successfully loaded into the buffer.

## Parameters / Member Variables
- : Pointer to the BufFile structure whose buffer needs to be loaded with data

## Dependencies
- Functions called/Symbols referenced:
  - FileRead (performs the actual file read operation)
  - FilePathName (gets file path for error reporting)
  - INSTR_TIME_SET_CURRENT (sets timing measurement points)
  - INSTR_TIME_SET_ZERO (initializes timing variables)
  - INSTR_TIME_ACCUM_DIFF (accumulates timing differences)
  - ereport (reports errors)
- Called from (representative examples):
  - BufFileReadCommon (main read operation entry point)

## Notes and Other Information
- This is a static (internal) function, not part of the public BufFile API
- Handles automatic file switching when MAX_PHYSICAL_FILESIZE is reached
- Includes comprehensive I/O timing tracking when track_io_timing is enabled
- Updates global pgBufferUsage statistics for monitoring temporary file I/O
- The function intentionally does not advance curOffset - this is handled by the caller
- Read errors are converted to PostgreSQL ERROR reports with appropriate file context
- The buffer size is fixed at compile time (sizeof(file->buffer.data))
- Used exclusively by the BufFile read path to maintain buffer state