# TempTablespacePath

## Location
[src/backend/storage/file/fd.c:1776-1800](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L1776-L1800)

## Overview
TempTablespacePath constructs the filesystem path for the temporary files directory within a specified tablespace.

## Definition

```c
void
TempTablespacePath(char *path, Oid tablespace)
```
## Detailed Description
This function generates the full filesystem path to the temporary files directory for a given tablespace. It handles both the default tablespace and user-defined tablespaces differently:

- For the default tablespace (InvalidOid, DEFAULTTABLESPACE_OID) or global tablespace (GLOBALTABLESPACE_OID), it creates a path under the "base" directory
- For custom tablespaces, it constructs a path through the "pg_tblspc" symbolic link structure

The function ensures that temporary files are organized properly within PostgreSQL's directory structure and provides a consistent interface for locating temp file storage areas across different tablespaces.

## Parameters / Member Variables
- : Output buffer that receives the constructed path string (must be at least MAXPGPATH bytes)
- : OID of the target tablespace, or special values like InvalidOid for default tablespace

## Dependencies
- Functions called/Symbols referenced:
  - PG_TEMP_FILES_DIR (constant for temp directory name)
  - TABLESPACE_VERSION_DIRECTORY (constant for tablespace version directory)
  - snprintf (standard C library function)

- Called from (representative examples):
  - [OpenTemporaryFileInTablespace](../O/OpenTemporaryFileInTablespace.md)
  - [FileSetCreate](../F/FileSetCreate.md)
  - [FileSetPath](../F/FileSetPath.md)
  - [pg_ls_tmpdir](../p/pg_ls_tmpdir.md)

## Notes and Other Information
- The function treats pg_global tablespace as equivalent to the default tablespace for temporary file placement
- Custom tablespaces use symbolic links under pg_tblspc/ to reference their actual storage locations
- The output path buffer must be pre-allocated with sufficient space (MAXPGPATH bytes)
- This function is essential for PostgreSQL's temporary file management across multiple tablespaces

## Simplified Source

```c
void
TempTablespacePath(char *path, Oid tablespace)
{
    // Check if this is the default or global tablespace
    if (tablespace == InvalidOid ||
        tablespace == DEFAULTTABLESPACE_OID ||
        tablespace == GLOBALTABLESPACE_OID) {
        // Use base directory for default tablespace
        snprintf(path, MAXPGPATH, "base/%s", PG_TEMP_FILES_DIR);
    } else {
        // Use symlink path for custom tablespaces
        snprintf(path, MAXPGPATH, "pg_tblspc/%u/%s/%s",
                tablespace, TABLESPACE_VERSION_DIRECTORY, PG_TEMP_FILES_DIR);
    }
}
```