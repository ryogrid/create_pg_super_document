# FilePath

## Location
src/backend/storage/file/fileset.c: 197 - 203

## Overview
The FilePath function computes the complete path to a specific file within a FileSet, combining directory path construction and tablespace selection.

## Definition
```c
static void
FilePath(char *path, FileSet *fileset, const char *name)
```

## Detailed Description
FilePath serves as a high-level utility function that constructs the full filesystem path to a named file within a FileSet. It orchestrates two key operations: first determining the appropriate tablespace for the file using ChooseTablespace(), then building the directory path using FileSetPath(), and finally combining these with the file name to create the complete path. This function abstracts the complexity of PostgreSQL's distributed temporary file storage system, providing a clean interface for file path resolution.

The function is static and serves as an internal utility within the fileset.c module, used by various FileSet operations that need to access specific files.

## Parameters / Member Variables
- `path`: Output buffer to store the constructed complete file path (must be at least MAXPGPATH in size)
- `fileset`: Pointer to the FileSet structure containing configuration and metadata
- `name`: The name of the file within the FileSet for which to construct the path

## Dependencies
- Functions called/Symbols referenced:
  - FileSetPath (constructs the directory path for the FileSet)
  - ChooseTablespace (determines which tablespace the file should belong to)
  - FileSet (struct type containing FileSet metadata and configuration)
- Called from (representative examples):
  - FileSetCreate (when creating initial files in a FileSet)
  - FileSetOpen (when opening existing files within a FileSet)
  - FileSetDelete (when deleting specific files from a FileSet)

## Notes and Other Information
- This is a static function, only accessible within the fileset.c compilation unit
- The function combines both tablespace selection and path construction in a single operation
- The caller must ensure the path buffer is large enough (MAXPGPATH bytes)
- Used as a fundamental building block for FileSet file operations throughout PostgreSQL
- The resulting path includes both the FileSet directory structure and the specific file name
- Provides abstraction over the underlying distributed file storage implementation