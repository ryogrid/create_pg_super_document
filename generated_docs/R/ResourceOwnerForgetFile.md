# ResourceOwnerForgetFile

## Location
[src/backend/storage/file/fd.c:377-385](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L377-L385)

## Overview
Convenience wrapper function that unregisters a file descriptor from a resource owner, removing it from automatic cleanup tracking.

## Definition
static inline void ResourceOwnerForgetFile(ResourceOwner owner, File file)

## Detailed Description
ResourceOwnerForgetFile is a static inline convenience wrapper around the general ResourceOwnerForget function, specifically designed for untracking File descriptors. It converts the File handle to a Datum using Int32GetDatum and removes it from the provided ResourceOwner using the file-specific resource descriptor (file_resowner_desc). This function is called when a file is explicitly closed to remove it from the resource owner's tracking list, preventing double cleanup when the resource owner is released.

The function is the counterpart to ResourceOwnerRememberFile and is part of PostgreSQL's resource management system that ensures proper cleanup of resources while avoiding double-free scenarios.

## Parameters / Member Variables
- owner: The ResourceOwner from which the file should be removed from tracking
- file: The File handle (file descriptor) to be removed from automatic cleanup tracking

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerForget](ResourceOwnerForget.md)
  - [Int32GetDatum](../I/Int32GetDatum.md)
  - file_resowner_desc (resource descriptor for file cleanup)
- Called from (representative examples):
  - [FileClose](../F/FileClose.md)

## Notes and Other Information
- This is a static inline function, so it's only visible within the fd.c source file
- The function works in conjunction with ResourceOwnerRememberFile to manage file descriptor lifecycles
- Must be called when explicitly closing files to prevent the resource owner from attempting to close an already-closed file
- Part of PostgreSQL's defensive programming approach to prevent double-close errors and resource management bugs