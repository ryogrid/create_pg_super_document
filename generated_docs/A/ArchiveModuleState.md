# ArchiveModuleState

## Location
[src/include/archive/archive_module.h:20-28](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/archive/archive_module.h#L20-L28)

## Overview
ArchiveModuleState is a structure that provides a mechanism for archive modules to maintain private state data across callback invocations.

## Definition


## Detailed Description
ArchiveModuleState serves as a container for archive module private data, enabling archive modules to store and access module-specific state information that persists across different callback function calls. This structure is fundamental to the archive module interface, providing a standardized way for modules to maintain context and configuration data throughout their lifecycle.

The structure is designed to be opaque to the PostgreSQL core system while providing archive modules with a flexible mechanism to store any data they need to maintain their operational state.

## Parameters / Member Variables
- : A void pointer that can be used by archive modules to store arbitrary module-specific data. This pointer is passed to all archive module callbacks, allowing modules to maintain state across different operations such as startup, file archiving, configuration checking, and shutdown.

## Dependencies
- Functions called/Symbols referenced:
  - None (simple data structure)
- Called from (representative examples):
  - [shell_archive_configured](../s/shell_archive_configured.md) (src/backend/archive/shell_archive.c:46)
  - [shell_archive_file](../s/shell_archive_file.md) (src/backend/archive/shell_archive.c:57)
  - [shell_archive_shutdown](../s/shell_archive_shutdown.md) (src/backend/archive/shell_archive.c:139)
  - [LoadArchiveLibrary](../L/LoadArchiveLibrary.md) (src/backend/postmaster/pgarch.c:942)

## Notes and Other Information
- This structure is defined in src/include/archive/archive_module.h:20-28
- The structure is intentionally minimal and generic to provide maximum flexibility for different types of archive modules
- [Archive](Archive.md) modules are responsible for managing the lifecycle of any data pointed to by private_data
- The structure is passed as a parameter to all archive module callback functions defined in ArchiveModuleCallbacks