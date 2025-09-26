# FileSetPath

## Location
src/backend/storage/file/fileset.c: 172 - 185

## Overview
The FileSetPath function builds the complete path for the directory that holds the files backing a FileSet in a given tablespace.

## Definition

```c
static void
FileSetPath(char *path, FileSet *fileset, Oid tablespace)
```
## Detailed Description
FileSetPath constructs a standardized directory path for FileSet storage within PostgreSQL's temporary file system. It combines the tablespace-specific temporary directory path with a unique identifier based on the FileSet's creator process ID and number. The resulting path follows the pattern: , ensuring that each FileSet has a unique directory location that can be easily identified and managed.

The function is static and serves as an internal utility within the fileset.c module for consistent path generation across various FileSet operations.

## Parameters / Member Variables
- : Output buffer to store the constructed path string (must be at least MAXPGPATH in size)
- : Pointer to the FileSet structure containing creator_pid and number for unique identification
- : OID of the tablespace where the FileSet directory should be located

## Dependencies
- Functions called/Symbols referenced:
  - TempTablespacePath (constructs the base temporary directory path for the tablespace)
  - PG_TEMP_FILE_PREFIX (constant prefix for temporary files)
  - FileSet (struct type for fileset metadata)
- Called from (representative examples):
  - FileSetCreate (when creating a new FileSet)
  - FileSetDeleteAll (when cleaning up FileSet directories)
  - FilePath (when constructing paths to individual files within a FileSet)

## Notes and Other Information
- This is a static function, only accessible within the fileset.c compilation unit
- The path construction ensures uniqueness through the combination of process ID and FileSet number
- The caller must ensure the path buffer is large enough (MAXPGPATH bytes)
- Used as a building block for other FileSet path operations throughout the system