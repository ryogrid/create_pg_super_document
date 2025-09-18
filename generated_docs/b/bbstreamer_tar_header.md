# bbstreamer_tar_header

## Location
src/bin/pg_basebackup/bbstreamer_tar.c: 261 - 318

## Overview
Parses a tar file header block and extracts file metadata, determining whether the block represents a valid file header or marks the end of the archive.

## Definition
```c
static bool bbstreamer_tar_header(bbstreamer_tar_parser *mystreamer)
```

## Detailed Description
This function processes a complete tar header block (512 bytes) that has been buffered by the parser. It first checks if the block consists entirely of zero bytes, which indicates the end of the archive according to tar format specifications. If the block contains non-zero data, it parses the standard tar header fields including filename, file size, permissions, ownership, and file type. The function extracts this metadata into the bbstreamer_member structure and calculates the required padding bytes for proper tar block alignment. After successful parsing, it forwards the header block to the next bbstreamer in the chain with the appropriate context.

## Parameters / Member Variables
- `mystreamer`: Pointer to the tar parser containing the buffered header data and member information structure

## Dependencies
- Functions called/Symbols referenced:
  - strlcpy
  - [read_tar_number](../r/read_tar_number.md)
  - [tarPaddingBytesRequired](../t/tarPaddingBytesRequired.md)
  - [bbstreamer_content](bbstreamer_content.md)
  - TAR_BLOCK_SIZE
  - TAR_OFFSET_NAME, TAR_OFFSET_SIZE, TAR_OFFSET_MODE, TAR_OFFSET_UID, TAR_OFFSET_GID
  - TAR_OFFSET_TYPEFLAG, TAR_OFFSET_LINKNAME
  - TAR_FILETYPE_DIRECTORY, TAR_FILETYPE_SYMLINK
  - BBSTREAMER_MEMBER_HEADER
- Called from (representative examples):
  - [bbstreamer_tar_parser_content](bbstreamer_tar_parser_content.md)

## Notes and Other Information
- Returns true if a valid file header was found and processed, false if end-of-archive detected
- Requires exactly TAR_BLOCK_SIZE (512) bytes to be buffered before invocation
- Validates that the filename field is not empty, failing with pg_fatal if it is
- Handles both regular files and special file types (directories and symbolic links)
- Calculates padding bytes needed to align file content to tar block boundaries
- End-of-archive detection is based on tar standard: a block of all zero bytes
- Forwards the complete header block to the next processing stage with BBSTREAMER_MEMBER_HEADER context