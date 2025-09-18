# _PrintFileData

## Location
src/bin/pg_dump/pg_backup_tar.c: 562 - 588

## Overview
A tar format-specific function that reads and outputs the contents of a specific file from the tar archive to the archive handle's output stream.

## Definition
static void _PrintFileData(ArchiveHandle *AH, char *filename)

## Detailed Description
The _PrintFileData function is part of the tar backup format implementation used for extracting and outputting file data during restore operations. It opens a specific file within the tar archive, reads its contents in 4KB chunks, null-terminates each chunk for safety, and writes the data to the archive handle's output stream. The function handles the complete file reading process from opening to closing, ensuring proper resource management of the tar member handle.

## Parameters / Member Variables
- : Pointer to the ArchiveHandle containing archive context and output stream
- : Name of the file within the tar archive to read and output (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - tarOpen (opens a tar member for reading)
  - tarRead (reads data from tar member)
  - tarClose (closes tar member)
  - ahwrite (writes data to archive handle output)
  - lclContext (local context structure)
  - TAR_MEMBER (tar member structure)
- Called from (representative examples):
  - _PrintTocData (when outputting TOC entry data)

## Notes and Other Information
- Uses a 4KB buffer (4095 bytes + null terminator) for efficient file reading
- Null-terminates each read buffer to ensure safe string handling
- Early returns if filename is NULL, providing defensive programming
- Properly manages tar member resources by closing the file handle after reading
- Sets the file handle in the local context (ctx->FH) during the operation
- This is a static function specific to the tar backup format implementation
- Essential for restore operations where individual files need to be extracted and output from the tar archive