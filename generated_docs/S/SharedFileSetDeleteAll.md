# SharedFileSetDeleteAll

## Location
[src/backend/storage/file/sharedfileset.c:83-95](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/sharedfileset.c#L83-L95)

## Overview
Deletes all files contained within a shared file set, providing a way to clean up temporary files before the file set itself is destroyed.

## Definition
```c
void SharedFileSetDeleteAll(SharedFileSet *fileset)
```

## Detailed Description
SharedFileSetDeleteAll is a simple wrapper function that deletes all files currently stored within a SharedFileSet. It delegates the actual file deletion work to the underlying FileSetDeleteAll function, which handles the low-level file system operations required to remove all files and directories associated with the file set.

This function is typically used when a process needs to clean up temporary files before completion, or when reinitializing a shared file set for reuse. Unlike automatic cleanup that occurs when all processes detach, this function provides explicit control over when files are deleted.

The function is straightforward:
1. Calls the underlying FileSetDeleteAll function on the embedded FileSet structure
2. All files and directories associated with the shared file set are removed from the file system

## Parameters / Member Variables
- `fileset`: Pointer to the SharedFileSet structure whose files should be deleted

## Dependencies
- Functions called/Symbols referenced:
  - [FileSetDeleteAll](../F/FileSetDeleteAll.md)
- Called from (representative examples):
  - [ExecHashJoinReInitializeDSM](../E/ExecHashJoinReInitializeDSM.md) (Hash join reinitialization for DSM)

## Notes and Other Information
- This function does not affect the reference count or the SharedFileSet structure itself - only the files it contains
- The SharedFileSet remains valid after this operation and can continue to be used for creating new files
- This provides explicit cleanup control separate from the automatic cleanup that occurs when the last process detaches
- Commonly used in scenarios where the same SharedFileSet needs to be reused multiple times with fresh files
- The underlying FileSetDeleteAll handles all the complexity of removing files and directories from the temporary file system
- No synchronization is performed by this function - callers must ensure appropriate coordination if multiple processes might be accessing the file set