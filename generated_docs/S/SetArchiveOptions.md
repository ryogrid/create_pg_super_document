# SetArchiveOptions

## Location
src/bin/pg_dump/pg_backup_archiver.c: 266 - 278

## Overview
Configures an archive with dump and restore options, providing flexibility to work with either or both option sets by synthesizing missing options when needed.

## Definition
```c
void SetArchiveOptions(Archive *AH, DumpOptions *dopt, RestoreOptions *ropt)
```

## Detailed Description
The SetArchiveOptions function sets the operating parameters for a PostgreSQL dump archive by assigning dump and restore option structures to the archive handle. This function provides flexibility in option handling - if dump options are not provided but restore options are, it automatically synthesizes dump options from the restore options using the dumpOptionsFromRestoreOptions helper function.

The function stores references to the option structures within the archive handle, making them accessible throughout the archive's lifetime for various operations. This centralized option storage allows different parts of the archive processing code to access consistent configuration settings.

## Parameters / Member Variables
- `AH`: Pointer to the Archive structure to configure
- `dopt`: Pointer to DumpOptions structure (can be NULL)
- `ropt`: Pointer to RestoreOptions structure (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [dumpOptionsFromRestoreOptions](../d/dumpOptionsFromRestoreOptions.md)
  - DumpOptions
  - [RestoreOptions](../R/RestoreOptions.md)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_dump.c)
  - [main](../m/main.md) (in pg_restore.c)
  - [_CloseArchive](../C/_CloseArchive.md) (in pg_backup_tar.c)

## Notes and Other Information
- This is a public function in the pg_dump/pg_restore architecture
- The function handles the case where only restore options are provided by synthesizing dump options
- Options are stored by reference, not copied, so the original option structures must remain valid
- Used by both pg_dump and pg_restore utilities to configure archive behavior
- The option synthesis feature allows restore operations to work with appropriate dump-compatible settings
- This function must be called after archive creation but before archive operations begin