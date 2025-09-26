# BufFileReadCommon

## Location
src/backend/storage/file/buffile.c: 593 - 644

## Overview
BufFileReadCommon is the core internal function that implements buffered reading from BufFiles with flexible EOF and exact-read handling options.

## Definition

```c
static size_t
BufFileReadCommon(BufFile *file, void *ptr, size_t size, bool exact, bool eofOK)
```
## Detailed Description
BufFileReadCommon provides the fundamental read functionality for the BufFile system, similar to the standard library's fread() function but with enhanced error handling and PostgreSQL-specific behavior. This internal function serves as the foundation for all public BufFile read operations.

The function implements a buffered reading strategy:
1. **Buffer management**: Maintains an internal buffer and loads data as needed from underlying files
2. **Multi-chunk reads**: Handles read requests larger than the buffer size by reading in chunks
3. **Position tracking**: Maintains accurate file position across buffer reloads
4. **Flexible completion**: Supports both partial reads and exact-size requirements based on parameters

Key behavioral aspects:
- Always flushes any pending writes before reading to ensure data consistency
- Automatically loads new buffer contents when current buffer is exhausted
- Handles end-of-file conditions gracefully or with errors based on caller requirements
- Optimizes memory copying by reading directly from buffer when data is available
- Advances file position accurately across buffer boundaries

## Parameters / Member Variables
- : Pointer to the BufFile structure to read from
- : Destination buffer to store the read data
- : Number of bytes to attempt to read
- : If true, requires exactly 'size' bytes to be read (no short reads allowed)
- : If true (and exact is true), allows zero-byte reads at end-of-file without error

## Dependencies
- Functions called/Symbols referenced:
  - BufFileFlush (ensures any pending writes are completed before reading)
  - BufFileLoadBuffer (loads fresh data into buffer when needed)
  - memcpy (copies data from buffer to destination)
  - ereport (reports errors for incomplete reads when exact=true)
- Called from (representative examples):
  - BufFileRead (standard read operation)
  - BufFileReadExact (read requiring exact byte count)
  - BufFileReadMaybeEOF (read allowing EOF conditions)

## Notes and Other Information
- This is a static (internal) function that provides the common implementation for all BufFile read variants
- Always flushes pending writes first to maintain consistency between read and write operations
- The function handles reads larger than the buffer size by reading multiple buffer loads
- Returns the actual number of bytes read, which may be less than requested unless exact=true
- Error handling distinguishes between named file sets and anonymous temporary files
- The exact/eofOK parameter combination allows fine-grained control over EOF behavior
- Used as the foundation for all BufFile read operations, providing consistent behavior across the API
- Optimizes performance by minimizing file I/O through intelligent buffer management