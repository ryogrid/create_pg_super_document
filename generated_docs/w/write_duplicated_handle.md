# write_duplicated_handle

## Location
[src/backend/postmaster/launch_backend.c:795-824](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/launch_backend.c#L795-L824)

## Overview
Duplicates a Windows handle for usage in a child process and stores the duplicated handle for writing to the backend parameter file.

## Definition
```c
static bool write_duplicated_handle(HANDLE *dest, HANDLE src, HANDLE childProcess)
```

## Detailed Description
This Windows-specific function creates a duplicate of a handle that can be inherited by a child process. It uses the Windows API `DuplicateHandle()` function to create a copy of the source handle in the context of the child process. The function ensures the duplicated handle has the same access rights as the original and can be inherited by the child process. If duplication fails, it logs an error message with the Windows error code.

## Parameters / Member Variables
- `dest`: Pointer to a HANDLE variable where the duplicated handle will be stored
- `src`: The source handle to be duplicated
- `childProcess`: Handle to the child process that will inherit the duplicated handle

## Dependencies
- Functions called/Symbols referenced:
  - DuplicateHandle (Windows API)
  - GetCurrentProcess (Windows API) 
  - GetLastError (Windows API)
  - ereport
  - LOG
  - [errmsg_internal](../e/errmsg_internal.md)
- Called from (representative examples):
  - [save_backend_variables](../s/save_backend_variables.md)

## Notes and Other Information
- This is a Windows-specific function (only compiled on Windows platforms)
- Uses DUPLICATE_CLOSE_SOURCE flag to close the source handle after duplication
- Uses DUPLICATE_SAME_ACCESS flag to preserve the same access rights
- The duplicated handle is marked as inheritable (TRUE parameter)
- Returns false on failure and logs the specific Windows error code for debugging
- Part of the backend launch mechanism in PostgreSQL on Windows