# internal_load_library

## Location
[src/backend/utils/fmgr/dfmgr.c:184-305](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/dfmgr.c#L184-L305)

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
  - [dlopen](../d/dlopen.md)
  - [dlerror](../d/dlerror.md)
  - [dlsym](../d/dlsym.md)
  - [dlclose](../d/dlclose.md)
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

## Simplified Source

```c
// Simplified version of internal_load_library
static void *internal_load_library(const char *libname) {
    DynamicFileList *file_scanner;
    PGModuleMagicFunction magic_func;
    struct stat stat_buf;
    PG_init_t PG_init;

    // Step 1: Check if library is already loaded by name
    for (file_scanner = file_list; file_scanner != NULL; file_scanner = file_scanner->next) {
        if (strcmp(libname, file_scanner->filename) == 0) {
            break;
        }
    }

    // Step 2: If not found by name, check for same file via different paths (symlinks)
    if (file_scanner == NULL) {
        if (stat(libname, &stat_buf) == -1) {
            ereport(ERROR, (errmsg("could not access file \"%s\"", libname)));
        }

        for (file_scanner = file_list; file_scanner != NULL; file_scanner = file_scanner->next) {
            if (SAME_INODE(stat_buf, *file_scanner)) {
                break;
            }
        }
    }

    // Step 3: Load new library if not already loaded
    if (file_scanner == NULL) {
        // Allocate new file entry
        file_scanner = malloc(offsetof(DynamicFileList, filename) + strlen(libname) + 1);
        if (file_scanner == NULL) {
            ereport(ERROR, (errmsg("out of memory")));
        }

        // Initialize file entry
        MemSet(file_scanner, 0, offsetof(DynamicFileList, filename));
        strcpy(file_scanner->filename, libname);
        file_scanner->device = stat_buf.st_dev;
        file_scanner->inode = stat_buf.st_ino;
        file_scanner->next = NULL;

        // Load the dynamic library
        file_scanner->handle = dlopen(file_scanner->filename, RTLD_NOW | RTLD_GLOBAL);
        if (file_scanner->handle == NULL) {
            char *load_error = dlerror();
            free(file_scanner);
            ereport(ERROR, (errmsg("could not load library \"%s\": %s", libname, load_error)));
        }

        // Step 4: Verify library compatibility via magic function
        magic_func = (PGModuleMagicFunction) dlsym(file_scanner->handle, PG_MAGIC_FUNCTION_NAME_STRING);
        if (magic_func) {
            const Pg_magic_struct *magic_data_ptr = (*magic_func)();
            if (magic_data_ptr->len != magic_data.len ||
                memcmp(magic_data_ptr, &magic_data, magic_data.len) != 0) {
                // Incompatible module - cleanup and report error
                dlclose(file_scanner->handle);
                free(file_scanner);
                incompatible_module_error(libname, magic_data_ptr);
            }
        } else {
            // Missing magic block - cleanup and report error
            dlclose(file_scanner->handle);
            free(file_scanner);
            ereport(ERROR, (errmsg("incompatible library \"%s\": missing magic block", libname)));
        }

        // Step 5: Call library initialization function if present
        PG_init = (PG_init_t) dlsym(file_scanner->handle, "_PG_init");
        if (PG_init) {
            (*PG_init)();
        }

        // Step 6: Add to loaded library list
        if (file_list == NULL) {
            file_list = file_scanner;
        } else {
            file_tail->next = file_scanner;
        }
        file_tail = file_scanner;
    }

    return file_scanner->handle;
}
```

Key simplifications made:
- Restructured nested conditions into clear sequential steps
- Consolidated error handling patterns while preserving critical checks
- Added descriptive comments for each major phase
- Simplified loop conditions for better readability
- Removed platform-specific conditional compilation details
- Focused on the main execution flow while preserving all essential logic
- Maintained all critical error conditions and cleanup operations