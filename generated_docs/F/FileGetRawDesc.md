# FileGetRawDesc

## Location
[src/backend/storage/file/fd.c:2474-2483](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L2474-L2483)

## Overview
FileGetRawDesc returns the raw operating system file descriptor associated with a PostgreSQL File, providing direct access to the underlying system file handle.

## Definition


## Detailed Description
FileGetRawDesc provides access to the underlying operating system file descriptor for a PostgreSQL File. This function bypasses PostgreSQL's virtual file descriptor abstraction layer to return the actual system file descriptor stored in the VfdCache. The function is used when direct system-level file operations are needed, such as for direct I/O operations or system calls that require a raw file descriptor.

The function includes important caveats: the returned file descriptor is valid only until the file is closed, and many operations within PostgreSQL can cause file closure (such as cache evictions in the VFD system). Therefore, callers must use the returned descriptor immediately and avoid other PostgreSQL operations that might invalidate it.

## Parameters / Member Variables
- : A PostgreSQL File descriptor representing an open file in the virtual file descriptor system

## Dependencies
- Functions called/Symbols referenced:
  - FileIsValid (validates the file descriptor)
  - VfdCache (global virtual file descriptor cache array)
- Called from (representative examples):
  - PG_O_DIRECT (for direct I/O operations)

## Notes and Other Information
- The function includes an assertion to validate the file descriptor using FileIsValid
- The returned file descriptor is the actual system file descriptor and should be used with caution
- Callers must not perform other PostgreSQL file operations before finishing with the raw descriptor
- The raw descriptor can become invalid due to VFD cache management operations
- This function breaks the abstraction provided by PostgreSQL's VFD system and should be used sparingly
- Primarily used for operations requiring direct system calls or special file attributes like O_DIRECT