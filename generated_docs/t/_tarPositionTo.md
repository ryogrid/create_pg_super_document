# _tarPositionTo

## Location
src/bin/pg_dump/pg_backup_tar.c: 1066 - 1139

## Overview
A static function that locates a specific file within a TAR archive, reads its header, and positions the file pointer to the beginning of the file data.

## Definition
```c
static TAR_MEMBER *_tarPositionTo(ArchiveHandle *AH, const char *filename)
```

## Detailed Description
This function performs TAR archive navigation to locate and position to a specific file or the next available file. It handles sequential scanning through TAR members, validating each header and skipping files until the target is found. When a filename is specified, it searches for an exact match; when filename is NULL, it returns the next available member. The function enforces data restoration order requirements and calculates proper positioning including TAR block padding. It maintains archive position tracking and sets up the TAR_MEMBER structure for subsequent read operations.

## Parameters / Member Variables
- `AH`: Archive handle containing the TAR file stream and context information
- `filename`: Target filename to locate, or NULL to get the next available member

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc0_object (zero-initialized memory allocation)
  - _tarReadRaw (raw TAR archive reading)
  - _tarGetHeader (TAR header parsing)
  - TocIDRequired (checks data requirements)
  - tarPaddingBytesRequired (calculates block padding)
  - pg_log_debug (debug logging)
  - lclContext (local context struct type)
  - TAR_MEMBER (TAR member struct type)
  - TAR_BLOCK_SIZE/REQ_DATA (constants)
- Called from (representative examples):
  - tarOpen (during archive opening/reading)

## Notes and Other Information
- Returns a newly allocated TAR_MEMBER structure on success, NULL if no more members
- Enforces sequential data restoration order - prevents out-of-order access
- Handles TAR block alignment by skipping padding bytes between members  
- Updates archive position tracking for efficient sequential access
- Provides detailed debug logging for archive navigation operations
- Supports both targeted file search and sequential scanning modes
- Located in src/bin/pg_dump/pg_backup_tar.c:1066-1139