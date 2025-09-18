# ArchiveModuleCallbacks

## Location
src/include/archive/archive_module.h: 43 - 49

## Overview
ArchiveModuleCallbacks is a structure that defines the callback function interface for PostgreSQL archive modules, containing function pointers for the various lifecycle operations of archive modules.

## Definition
```c
typedef struct ArchiveModuleCallbacks
{
    ArchiveStartupCB startup_cb;
    ArchiveCheckConfiguredCB check_configured_cb;
    ArchiveFileCB archive_file_cb;
    ArchiveShutdownCB shutdown_cb;
} ArchiveModuleCallbacks;
```

## Detailed Description
ArchiveModuleCallbacks defines the standard interface that archive modules must implement to integrate with PostgreSQL's archiving system. This structure contains function pointers for all the callback functions that an archive module can provide. The callbacks handle different phases of the archiving lifecycle, from module initialization through file archiving to shutdown.

Archive libraries define these callback functions and return them via the _PG_archive_module_init() function. Of all the callbacks, only ArchiveFileCB (archive_file_cb) is required; the others are optional and can be NULL if not needed by the specific archive module implementation.

## Parameters / Member Variables
- `startup_cb`: Function pointer of type ArchiveStartupCB - called during archive module initialization to perform any necessary startup operations
- `check_configured_cb`: Function pointer of type ArchiveCheckConfiguredCB - called to verify that the archive module is properly configured and ready to archive files
- `archive_file_cb`: Function pointer of type ArchiveFileCB - the core callback responsible for actually archiving WAL files (this is the only required callback)
- `shutdown_cb`: Function pointer of type ArchiveShutdownCB - called during archive module shutdown to perform cleanup operations

## Dependencies
- Functions called/Symbols referenced:
  - ArchiveStartupCB (typedef void (*)(ArchiveModuleState *state))
  - ArchiveCheckConfiguredCB (typedef bool (*)(ArchiveModuleState *state))
  - ArchiveFileCB (typedef bool (*)(ArchiveModuleState *state, const char *file, const char *path))
  - ArchiveShutdownCB (typedef void (*)(ArchiveModuleState *state))
- Called from (representative examples):
  - _SHELL_ARCHIVE_H (src/include/archive/shell_archive.h:22)

## Notes and Other Information
- This structure is defined in src/include/archive/archive_module.h:43-49
- The callback types are defined as function pointer typedefs in the same header file (lines 38-41)
- ArchiveFileCB is the only mandatory callback; others can be NULL if not required
- All callbacks receive an ArchiveModuleState pointer to access module-specific state data
- The structure is returned by archive modules through the _PG_archive_module_init() function
- [Archive](Archive.md) modules should populate this structure with their specific callback implementations