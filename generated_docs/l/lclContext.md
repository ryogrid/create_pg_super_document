# lclContext

## Location
src/bin/pg_dump/pg_backup_custom.c: 75 - 80

## Overview
The `lclContext` structure serves as a local context for PostgreSQL custom archive format operations, maintaining state information for compression handling and file positioning during backup and restore operations.

## Definition
```c
typedef struct
{
    CompressorState *cs;
    int         hasSeek;
    /* lastFilePos is used only when reading, and may be invalid if !hasSeek */
    pgoff_t     lastFilePos;    /* position after last data block we've read */
} lclContext;
```

## Detailed Description
The `lclContext` structure is a private data structure used by the pg_dump custom archive format to maintain local state during archive operations. It is allocated and stored in the `ArchiveHandle->formatData` field when initializing a custom format archive. The structure manages compression state and file positioning information needed for efficient reading and writing of custom format archive files.

This context is used throughout the custom archive implementation to track the current state of compression operations and maintain file positioning information for seek operations when the underlying file system supports them.

## Parameters / Member Variables
- `cs`: Pointer to a CompressorState structure that manages compression/decompression operations for the archive data
- `hasSeek`: Integer flag indicating whether the underlying file supports seek operations (non-zero if seeking is available)
- `lastFilePos`: File position (of type pgoff_t) tracking the position after the last data block read; only valid during read operations and may be invalid if hasSeek is false

## Dependencies
- Functions called/Symbols referenced:
  - pgoff_t (for file position tracking)
  - [CompressorState](../C/CompressorState.md) (for compression handling)
- Called from (representative examples):
  - [InitArchiveFmt_Custom](../I/InitArchiveFmt_Custom.md) (allocates and initializes lclContext)
  - [_StartData](../S/_StartData.md) (accesses context for data operations)
  - [_WriteData](../W/_WriteData.md) (uses context for writing data)
  - [_EndData](../E/_EndData.md) (references context for data finalization)
  - [_getFilePos](../g/_getFilePos.md) (uses context to determine file position capabilities)
  - [_Clone](../C/_Clone.md) (copies context during parallel operations)
  - [_DeClone](../D/_DeClone.md) (cleans up cloned context)

## Notes and Other Information
- The structure is allocated using pg_malloc0() during archive initialization, ensuring all fields start with zero values
- The lastFilePos field is specifically noted in comments as being used only during reading operations
- The hasSeek capability affects how file positioning is handled - when seeking is not available, some operations may be limited
- This context is part of the private implementation details of the custom archive format and is not exposed to external callers
- The structure is used across multiple archive backends (custom, directory, tar) as evidenced by the reference patterns
- File positioning is critical for the custom format's ability to rewrite the Table of Contents (TOC) with data block offsets