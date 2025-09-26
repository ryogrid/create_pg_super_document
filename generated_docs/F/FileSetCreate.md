# FileSetCreate

## Location
[src/backend/storage/file/fileset.c:92-118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fileset.c#L92-L118)

## Overview
Creates a new temporary file within a FileSet, automatically handling directory creation if needed and distributing files across configured tablespaces.

## Definition

```c
File
FileSetCreate(FileSet *fileset, const char *name)
```
## Detailed Description
FileSetCreate creates a new temporary file with the specified name within the given FileSet. The function first constructs the complete file path using the fileset's tablespace distribution strategy, then attempts to create the temporary file at that location.

If the initial file creation fails (typically because the directory doesn't exist yet), the function automatically creates the necessary directory structure on demand. This includes creating both the temporary directory path for the chosen tablespace and the specific FileSet directory within it, then retrying the file creation.

The function uses PostgreSQL's temporary file management system, which provides automatic cleanup and proper integration with the backend's resource management. Files are distributed across tablespaces according to the fileset's configuration for load balancing and storage optimization.

## Parameters / Member Variables
- : Pointer to the initialized FileSet structure that manages the file collection
- : Name of the file to create within the fileset (used for path construction)

## Dependencies
- Functions called/Symbols referenced:
  - FilePath: Constructs the complete file path for the given fileset and name
  - PathNameCreateTemporaryFile: Creates the temporary file at the specified path
  - ChooseTablespace: Selects an appropriate tablespace for the file based on fileset configuration
  - TempTablespacePath: Constructs the temporary directory path for a tablespace
  - FileSetPath: Constructs the fileset-specific directory path
  - PathNameCreateTemporaryDir: Creates the directory structure on demand
  - MAXPGPATH: Maximum path length constant

- Called from (representative examples):
  - MakeNewFileSetSegment: Used in BufFile management for creating file segments

## Notes and Other Information
- Returns a File descriptor that can be used with PostgreSQL's file management APIs
- Implements lazy directory creation - directories are only created when the first file creation attempt fails
- The function handles both the temporary tablespace directory and fileset-specific subdirectory creation
- File creation follows PostgreSQL's temporary file naming conventions and cleanup mechanisms
- The  parameter in the second PathNameCreateTemporaryFile call is set to  to ensure proper error reporting if directory creation doesn't resolve the issue
- Files created are automatically managed by PostgreSQL's resource cleanup system
- The tablespace selection is handled by ChooseTablespace, which implements the distribution strategy across available tablespaces