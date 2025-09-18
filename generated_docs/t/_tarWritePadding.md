# _tarWritePadding

## Location
src/backend/backup/basebackup.c: 2071 - 2093

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
  - bbsink_archive_contents
  - TAR_BLOCK_SIZE
  - bbsink
- Called from (representative examples):
  - [sendFileWithContent](../s/sendFileWithContent.md)
  - [sendFile](../s/sendFile.md)

## Notes and Other Information
- Static function used only within the basebackup.c module
- Essential for maintaining TAR format compliance during backup operations
- Uses assertions to ensure buffer capacity is sufficient for single-operation padding
- Padding is always with zero bytes as required by TAR format specification
- Works in conjunction with _tarWriteHeader to create properly formatted TAR archives