# ResourceOwnerRememberFile

## Location
[src/backend/storage/file/fd.c:372-376](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L372-L376)

## Overview
Convenience wrapper function that registers a file descriptor with a resource owner for automatic cleanup when the resource owner is released.

## Definition
static inline void ResourceOwnerRememberFile(ResourceOwner owner, File file)

## Detailed Description
ResourceOwnerRememberFile is a static inline convenience wrapper around the general ResourceOwnerRemember function, specifically designed for tracking File descriptors. It converts the File handle to a Datum using Int32GetDatum and registers it with the provided ResourceOwner using the file-specific resource descriptor (file_resowner_desc). This ensures that files are properly closed and cleaned up when the resource owner context is released, preventing file descriptor leaks.

The function is part of PostgreSQL's resource management system that tracks various resources (memory, files, locks, etc.) and ensures they are properly released when transactions abort or complete.

## Parameters / Member Variables
- owner: The ResourceOwner that should track this file descriptor
- file: The File handle (file descriptor) to be tracked for automatic cleanup

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerRemember](ResourceOwnerRemember.md)
  - [Int32GetDatum](../I/Int32GetDatum.md)
  - file_resowner_desc (resource descriptor for file cleanup)
- Called from (representative examples):
  - [RegisterTemporaryFile](RegisterTemporaryFile.md)

## Notes and Other Information
- This is a static inline function, so it's only visible within the fd.c source file
- The function works in conjunction with ResourceOwnerForgetFile to manage file descriptor lifecycles
- The file_resowner_desc structure defines how files should be released (after locks, with specific priority)
- Part of PostgreSQL's defensive programming approach to prevent resource leaks