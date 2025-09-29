# _tarWritePadding

## Location
[src/backend/backup/basebackup.c:2071-2093](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup.c#L2071-L2093)

## Overview
Writes zero-byte padding to a TAR archive to align file data to TAR block size boundaries, ensuring proper TAR format compliance.

## Definition
```c
static void _tarWritePadding(bbsink *sink, int len)
```

## Detailed Description
This function ensures TAR format compliance by padding file data with zero bytes to align to TAR block boundaries. TAR format requires that all file data be padded to multiples of TAR_BLOCK_SIZE (512 bytes). The function calculates the necessary padding bytes and writes them to the backup sink. It includes buffer size assertions to ensure the operation can be completed in a single write without requiring multiple chunks.

## Parameters / Member Variables
- `sink`: Backup sink object that manages the output stream for the backup data
- `len`: Length of the data that needs to be padded to a TAR block boundary

## Dependencies
- Functions called/Symbols referenced:
  - [tarPaddingBytesRequired](tarPaddingBytesRequired.md)
  - MemSet
  - [bbsink_archive_contents](../b/bbsink_archive_contents.md)
  - TAR_BLOCK_SIZE
  - [bbsink](../b/bbsink.md)
- Called from (representative examples):
  - [sendFileWithContent](../s/sendFileWithContent.md)
  - [sendFile](../s/sendFile.md)

## Notes and Other Information
- Static function used only within the basebackup.c module
- Essential for maintaining TAR format compliance during backup operations
- Uses assertions to ensure buffer capacity is sufficient for single-operation padding
- Padding is always with zero bytes as required by TAR format specification
- Works in conjunction with _tarWriteHeader to create properly formatted TAR archives

## Simplified Source

```c
// Write zero-byte padding to align TAR entries to block boundaries
static void _tarWritePadding(bbsink *sink, int len)
{
    int pad = tarPaddingBytesRequired(len);

    /*
     * Buffer should be large enough for single-operation padding.
     * TAR_BLOCK_SIZE is typically 512 bytes.
     */
    Assert(sink->bbs_buffer_length >= TAR_BLOCK_SIZE);
    Assert(pad <= TAR_BLOCK_SIZE);

    if (pad > 0) {
        // Fill buffer with zeros and write to sink
        MemSet(sink->bbs_buffer, 0, pad);
        bbsink_archive_contents(sink, pad);
    }
}
```

**Key Points:**
- Calculates padding needed using tarPaddingBytesRequired()
- Fills sink buffer with zero bytes for required padding length
- Ensures TAR format compliance by aligning to TAR_BLOCK_SIZE boundaries
- Uses assertions to verify buffer capacity for single-operation write
- Only writes padding if actually needed (pad > 0)