# WalWriteMethodOps

## Location
src/bin/pg_basebackup/walmethods.h: 41 - 93

## Overview
WalWriteMethodOps is a table of callback functions that defines the interface for WAL writing operations in PostgreSQL backup utilities.

## Definition
```c
typedef struct WalWriteMethodOps
{
    Walfile *(*open_for_write) (WalWriteMethod *wwmethod, const char *pathname, const char *temp_suffix, size_t pad_to_size);
    int (*close) (Walfile *f, WalCloseMethod method);
    bool (*existsfile) (WalWriteMethod *wwmethod, const char *pathname);
    ssize_t (*get_file_size) (WalWriteMethod *wwmethod, const char *pathname);
    char *(*get_file_name) (WalWriteMethod *wwmethod, const char *pathname, const char *temp_suffix);
    ssize_t (*write) (Walfile *f, const void *buf, size_t count);
    int (*sync) (Walfile *f);
    bool (*finish) (WalWriteMethod *wwmethod);
    void (*free) (WalWriteMethod *wwmethod);
} WalWriteMethodOps;
```

## Detailed Description
WalWriteMethodOps implements a vtable pattern to provide polymorphic behavior for different WAL writing methods. This structure contains function pointers that define the complete interface for WAL file operations including opening, writing, syncing, and closing files. Each implementation (like directory-based or tar-based writing) provides its own concrete implementations of these function pointers.

The structure enables a clean separation between the generic WAL writing logic and the specific implementation details of how WAL data is stored. This design allows PostgreSQL backup utilities to support multiple output formats while maintaining a consistent programming interface.

## Parameters / Member Variables
- `open_for_write`: Opens a target file for writing, with optional temp suffix and padding support
- `close`: Closes an open Walfile using specified close method (normal, unlink, no rename)
- `existsfile`: Checks if a file exists at the given pathname
- `get_file_size`: Returns the size of a file, or -1 on failure
- `get_file_name`: Returns the current file name without base directory (useful for logging)
- `write`: Writes specified number of bytes to the file, returns bytes written or -1 for error
- `sync`: Performs fsync operation on the file contents, returns 0 on success
- `finish`: Cleans up shared resources and finalizes the method (e.g., writing tar headers)
- `free`: Frees subsidiary data and the WalWriteMethod structure itself

## Dependencies
- Functions called/Symbols referenced:
  - Walfile
  - WalWriteMethod
  - WalCloseMethod
  - ssize_t

- Called from (representative examples):
  - WalDirectoryMethodOps (src/bin/pg_basebackup/walmethods.c:58)
  - WalTarMethodOps (src/bin/pg_basebackup/walmethods.c:679)
  - WalWriteMethod.ops field (src/bin/pg_basebackup/walmethods.h:105)

## Notes and Other Information
- Two concrete implementations exist: WalDirectoryMethodOps for regular file operations and WalTarMethodOps for tar archive operations
- The open_for_write function supports temporary file creation with automatic renaming on close
- Error handling is managed through the parent WalWriteMethod structure's error fields
- The finish operation is crucial for tar methods as it writes final headers and metadata
- All implementations must handle the case where the underlying WalWriteMethod may have method-specific data following the base structure