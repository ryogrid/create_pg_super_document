# GetIncrementalFilePath

## Location
src/backend/backup/basebackup_incremental.c: 627 - 666

## Overview
Generates the destination pathname for a file when it needs to be sent as part of an incremental backup, creating an "INCREMENTAL." prefixed filename in the same directory.

## Definition


## Detailed Description
This function constructs the pathname that should be used when a database file is being sent incrementally during a backup. It takes the normal relation file path and transforms it into an incremental backup path by:

1. Getting the standard relation file path using GetRelationPath
2. Splitting the path at the last directory separator
3. Creating a new filename in the same directory with the "INCREMENTAL." prefix
4. Appending the segment number if the file is segmented (segno > 0)

The resulting path allows incremental backup files to be stored alongside the original files while being clearly identified as incremental backup content. For example, a file "base/16384/12345" would become "base/16384/INCREMENTAL.12345" or "base/16384/INCREMENTAL.12345.1" for segmented files.

## Parameters / Member Variables
- : Database OID for the relation
- : Tablespace OID for the relation  
- : File number of the relation
- : Fork number (main, fsm, vm, etc.)
- : Segment number for large relations (0 for unsegmented files)

## Dependencies
- Functions called/Symbols referenced:
  - GetRelationPath: Gets the standard filesystem path for a relation file
  - strrchr: Finds the last directory separator in the path
  - psprintf: Formats the incremental path string
  - pfree: Frees the temporary path memory
- Constants referenced:
  - INVALID_PROC_NUMBER: Used when calling GetRelationPath
- Called from:
  - GetFileBackupMethod (src/backend/backup/basebackup_incremental.c:733)

## Notes and Other Information
- Returns a palloc'd string that must be freed by the caller
- The "INCREMENTAL." prefix clearly identifies files as incremental backup content
- Handles both segmented and non-segmented files appropriately
- Preserves the directory structure of the original file
- The function assumes the original path contains at least one directory separator
- Used in conjunction with GetFileBackupMethod to determine how files should be handled during incremental backups