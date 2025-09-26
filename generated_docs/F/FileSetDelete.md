# FileSetDelete

## Location
[src/backend/storage/file/fileset.c:136-149](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fileset.c#L136-L149)

## Overview
Deletes a specific temporary file from a FileSet, with configurable error handling for cases where the file doesn't exist.

## Definition

```c
bool
FileSetDelete(FileSet *fileset, const char *name,
			  bool error_on_failure)
```
## Detailed Description
FileSetDelete removes a specific temporary file from a FileSet that was previously created using FileSetCreate. The function provides controlled deletion with optional error handling, allowing callers to specify whether missing files should trigger errors or be silently ignored.

The function constructs the complete file path using the same path generation logic as FileSetCreate and FileSetOpen, ensuring consistent file location across all FileSet operations. It then delegates the actual file deletion to PostgreSQL's temporary file management system.

This function is essential for explicit resource management in FileSet operations, as temporary files created through the FileSet API are not automatically cleaned up and must be explicitly deleted by the caller when no longer needed.

## Parameters / Member Variables
- : Pointer to the FileSet structure containing the file to delete
- : Name of the file to delete (must match the name used when creating the file)
- : Controls error behavior - if true, missing files cause errors; if false, missing files are silently ignored

**Return Value:**
- : File existed and was successfully deleted
- : File did not exist (when error_on_failure is false)

## Dependencies
- Functions called/Symbols referenced:
  - [FilePath](FilePath.md): Constructs the complete file path for the given fileset and name
  - [PathNameDeleteTemporaryFile](../P/PathNameDeleteTemporaryFile.md): Performs the actual file deletion operation
  - MAXPGPATH: Maximum path length constant

- Called from (representative examples):
  - [MakeNewFileSetSegment](../M/MakeNewFileSetSegment.md): Used in BufFile operations for cleaning up temporary segments
  - [BufFileDeleteFileSet](../B/BufFileDeleteFileSet.md): Used to delete specific files in buffered file operations
  - [BufFileTruncateFileSet](../B/BufFileTruncateFileSet.md): Used during file truncation operations

## Notes and Other Information
- Part of the explicit resource management strategy for FileSet - files are not automatically cleaned up
- The error_on_failure parameter provides flexibility in error handling depending on the caller's requirements
- Uses the same path construction logic as other FileSet operations to maintain consistency
- Returns a boolean indicating whether the file actually existed, useful for conditional logic in callers
- Does not remove the directory structure, only the specific file
- The function assumes the FileSet structure is valid and properly initialized
- Delegates actual deletion semantics to PostgreSQL's temporary file management system
- Critical for preventing resource leaks in long-running operations that create many temporary files