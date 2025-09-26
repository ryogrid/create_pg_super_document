# RemovePgTempRelationFiles

## Location
src/backend/storage/file/fd.c: 3390 - 3417

## Overview
Processes one tablespace directory to find and clean up temporary relation files in per-database subdirectories.

## Definition
```c
static void RemovePgTempRelationFiles(const char *tsdirname)
```

## Detailed Description
This function traverses a tablespace directory looking for per-database subdirectories (identified by numeric names representing database OIDs). For each valid database directory found, it calls `RemovePgTempRelationFilesInDbspace` to clean up temporary relation files within that database space.

The function specifically filters directories to only process those with purely numeric names, which correspond to PostgreSQL database OIDs. This filtering automatically ignores non-database directories like "." and ".." as well as any other non-numeric directory names that might exist in the tablespace.

## Parameters / Member Variables
- `tsdirname`: Path to the tablespace directory to process

## Dependencies
- Functions called/Symbols referenced:
  - AllocateDir
  - ReadDirExtended
  - RemovePgTempRelationFilesInDbspace
  - FreeDir
- Called from (representative examples):
  - RemovePgTempFiles

## Notes and Other Information
- This is a static function, only accessible within the fd.c source file
- Uses `strspn()` to validate that directory names contain only digits (0-9)
- Automatically skips "." and ".." directories due to the numeric name validation
- Part of the PostgreSQL temporary file cleanup hierarchy: this function handles the tablespace level, delegating database-specific cleanup to `RemovePgTempRelationFilesInDbspace`
- Database directories are identified by their OID (Object Identifier) which is always numeric