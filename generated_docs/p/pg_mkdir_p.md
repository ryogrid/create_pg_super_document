# pg_mkdir_p

## Location
[src/port/pgmkdirp.c:57-148](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pgmkdirp.c#L57-L148)

## Overview
A platform-independent function that creates a directory and any necessary parent directories, equivalent to the Unix "mkdir -p" command but without complaining if the target directory already exists.

## Definition

```c
struct stat sb;
```
## Detailed Description
The  function implements recursive directory creation functionality similar to the Unix "mkdir -p" command. It creates the specified directory along with any missing parent directories in the path. The function handles both Windows and POSIX systems, with special logic for Windows network drives and local drive specifications.

The function follows POSIX 1003.2 semantics by temporarily modifying the user's umask to ensure parent directories are created with appropriate permissions (preserving user write and execute permissions), while the final target directory receives the specified permissions. If any component in the path already exists but is not a directory, the function fails with appropriate error codes.

## Parameters / Member Variables
- : A null-terminated string containing the directory path to create. The path is assumed to be in canonical form using '/' as the separator. Note that this parameter may be modified on failure to show the problematic directory level.
- : File permissions for the target directory (declared as int rather than mode_t to minimize dependencies). Parent directories are created with umask-based permissions plus user write/execute bits.

## Dependencies
- Functions called/Symbols referenced:
  -  (system call for checking file/directory existence)
  -  (system call for creating directories)
  - 0022 (system call for getting/setting file creation mask)
  -  (string function for Windows network path parsing)
  - Standard file permission constants: , , , , , 
  -  type definition

- Called from (representative examples):
  -  (database recovery operations)
  -  (tablespace management)
  -  (initdb utility)
  -  (initdb utility)
  -  (pg_basebackup utility)
  -  (pg_basebackup utility)
  - Various other database utilities and backend operations

## Notes and Other Information
- **Return Value**: Returns 0 on success, -1 on failure with errno set appropriately
- **Error Handling**: On failure, the path argument is modified to indicate the specific directory level that caused the problem
- **Platform Support**: Includes special handling for Windows paths, supporting both UNC network paths (//server/share) and local drive specifications (C:)
- **Permission Handling**: Uses careful umask manipulation to ensure parent directories have sufficient permissions for creation while respecting the original umask for the final directory
- **Thread Safety**: The function temporarily modifies the process umask, which could affect other threads in multi-threaded environments
- **Path Requirements**: Assumes the input path uses forward slashes as separators and is in canonical form
- **Existing Directory Behavior**: Unlike standard mkdir, this function succeeds if the target directory already exists