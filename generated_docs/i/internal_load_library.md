# internal_load_library

## Location
src/backend/utils/fmgr/dfmgr.c: 184 - 305

## Overview
This function is the core library loading mechanism that handles dynamic library loading with duplicate detection, compatibility checking, and initialization for PostgreSQL's dynamic function management system.

## Definition
```c
static void *internal_load_library(const char *libname)
```

## Detailed Description
The `internal_load_library` function implements PostgreSQL's sophisticated dynamic library loading system. It maintains a list of loaded libraries to prevent duplicate loading and uses file system statistics (device and inode numbers) to detect when the same library is referenced through different paths (e.g., symlinks). 

The function performs several critical operations: it checks for already-loaded libraries, loads new libraries using `dlopen()`, validates library compatibility through the PG_MODULE_MAGIC system, and calls the library's initialization function (`_PG_init`) if present. The compatibility checking ensures that dynamically loaded modules were compiled against a compatible version of PostgreSQL and use the required magic block.

This is a static function that serves as the underlying implementation for both `load_external_function` and `load_file`.

## Parameters / Member Variables
- `libname`: The exact name/path of the library file to load (must be fully qualified, not abbreviated)

## Dependencies
- Functions called/Symbols referenced:
  - DynamicFileList (struct)
  - SAME_INODE (macro)
  - malloc
  - MemSet
  - dlopen
  - dlerror
  - dlsym
  - dlclose
  - [incompatible_module_error](incompatible_module_error.md)
  - PG_MAGIC_FUNCTION_NAME_STRING
  - Pg_magic_struct
- Called from (representative examples):
  - [load_external_function](../l/load_external_function.md)
  - [load_file](../l/load_file.md)
  - [RestoreLibraryState](../R/RestoreLibraryState.md)

## Notes and Other Information
- This function is part of PostgreSQL's dynamic function management system located in src/backend/utils/fmgr/dfmgr.c
- Maintains a global linked list (file_list) of loaded libraries to prevent duplicate loading
- Uses RTLD_NOW | RTLD_GLOBAL flags for dlopen() to ensure immediate symbol resolution and global symbol visibility
- Performs compatibility checking through the PG_MODULE_MAGIC system - libraries without proper magic blocks are rejected
- Automatically calls the library's `_PG_init()` function if present for library initialization
- Uses file system statistics (device/inode) to detect the same file referenced through different paths
- Memory allocation uses malloc() rather than PostgreSQL's memory management for the file list structure
- Currently there is no mechanism to unload dynamically loaded libraries (noted as a potential future enhancement)
- Static function - not directly accessible outside of dfmgr.c