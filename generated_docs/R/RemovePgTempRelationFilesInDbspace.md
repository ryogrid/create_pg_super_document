# RemovePgTempRelationFilesInDbspace

## Location
[src/backend/storage/file/fd.c:3418-3445](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L3418-L3445)

## Overview
Processes one per-database directory to remove temporary relation files by identifying and unlinking files that match temporary relation naming patterns.

## Definition
```c
static void RemovePgTempRelationFilesInDbspace(const char *dbspacedirname)
```

## Detailed Description
This function scans a database-specific directory within a tablespace to locate and remove temporary relation files. It identifies temporary relation files using the `looks_like_temp_rel_name()` function to validate file names against expected temporary relation naming patterns.

For each file that matches the temporary relation pattern, the function attempts to unlink (delete) it from the filesystem. If the unlink operation fails, it logs an error message but continues processing other files, ensuring that individual file removal failures do not halt the entire cleanup process.

This function operates at the most granular level of PostgreSQLs temporary file cleanup hierarchy, handling the actual removal of temporary relation files within a specific database space.

## Parameters / Member Variables
- `dbspacedirname`: Path to the database-specific directory within a tablespace to process

## Dependencies
- Functions called/Symbols referenced:
  - [AllocateDir](../A/AllocateDir.md)
  - [ReadDirExtended](ReadDirExtended.md)
  - [looks_like_temp_rel_name](../l/looks_like_temp_rel_name.md)
  - unlink
  - [FreeDir](../F/FreeDir.md)
- Called from (representative examples):
  - [RemovePgTempRelationFiles](RemovePgTempRelationFiles.md)

## Notes and Other Information
- This is a static function, only accessible within the fd.c source file
- Uses `looks_like_temp_rel_name()` to identify temporary relation files by their naming pattern
- Error handling is non-fatal - failed unlink operations are logged but do not stop processing
- Part of the PostgreSQL temporary file cleanup hierarchy at the database level
- Only processes files that match temporary relation naming conventions, ignoring other files in the directory
- The function assumes all entries that pass the `looks_like_temp_rel_name()` test are files (not directories)