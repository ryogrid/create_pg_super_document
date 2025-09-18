# shell_archive_init

## Location
[src/backend/archive/shell_archive.c:40-45](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/archive/shell_archive.c#L40-L45)

## Overview
This function serves as the initialization entry point for the shell-based WAL archiving module, returning a pointer to the callback structure that defines the module's archiving interface.

## Definition
```c
const ArchiveModuleCallbacks *shell_archive_init(void)
```

## Detailed Description
The `shell_archive_init` function is the standard initialization function for PostgreSQL's default shell-based WAL archiving module. It returns a pointer to a static `ArchiveModuleCallbacks` structure that contains function pointers for the various archiving operations. This function follows the PostgreSQL archive module interface pattern, where each archive module provides an initialization function that returns its callback structure.

The returned callbacks structure includes:
- `startup_cb`: Set to NULL (no startup processing needed)
- `check_configured_cb`: Points to `shell_archive_configured`
- `archive_file_cb`: Points to `shell_archive_file`
- `shutdown_cb`: Points to `shell_archive_shutdown`

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - shell_archive_callbacks (static structure)
- Called from (representative examples):
  - [LoadArchiveLibrary](../L/LoadArchiveLibrary.md) (in pgarch.c:926)

## Notes and Other Information
- This is the entry point function that PostgreSQL's archiver process calls to initialize the shell archiving module
- The function is declared in src/include/archive/shell_archive.h
- The shell archive module is PostgreSQL's default WAL archiving implementation that uses the `archive_command` GUC to execute user-specified shell commands for archiving WAL files
- The returned callback structure is statically allocated and remains valid for the lifetime of the process