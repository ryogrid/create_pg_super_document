# PathNameCreateTemporaryDir

## Location
[src/backend/storage/file/fd.c:1657-1687](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L1657-L1687)

## Overview
PathNameCreateTemporaryDir creates a temporary directory and its parent directory if necessary, designed specifically for PostgreSQL's temporary file management system with proper cleanup integration.

## Definition

```c
struct stat statbuf;
```
## Detailed Description
PathNameCreateTemporaryDir implements a two-level directory creation strategy for PostgreSQL's temporary file system. It first attempts to create the target directory, and if that fails due to a missing parent directory, it creates the base directory first before retrying. The function handles race conditions by tolerating EEXIST errors when multiple processes attempt to create the same directories simultaneously. It integrates with PostgreSQL's startup cleanup mechanism by ensuring directories follow the PG_TEMP_FILE_PREFIX naming convention so they can be identified and removed by RemovePgTempFiles() during database startup.

## Parameters / Member Variables
- : The parent directory path that must exist or be created
- : The target temporary directory to create (should begin with PG_TEMP_FILE_PREFIX for top-level temp dirs)

## Dependencies
- Functions called/Symbols referenced:
  - [MakePGDirectory](../M/MakePGDirectory.md)
  - ereport
  - [errcode_for_file_access](../e/errcode_for_file_access.md)
  - [errmsg](../e/errmsg.md)
- Called from (representative examples):
  - [FileSetCreate](../F/FileSetCreate.md)

## Notes and Other Information
This function is part of PostgreSQL's temporary file management infrastructure in src/backend/storage/file/fd.c. It's designed for creating the hierarchical temporary directory structure needed for operations like parallel query execution and large sorts. The function emphasizes robustness by handling missing parent directories and race conditions between concurrent processes. Top-level temporary directories should follow the PG_TEMP_FILE_PREFIX naming convention for automatic cleanup, while subdirectories don't require specific prefixes. The function returns void and uses PostgreSQL's error reporting mechanism for failure cases.