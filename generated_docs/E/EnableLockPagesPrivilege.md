# EnableLockPagesPrivilege

## Location
src/backend/port/win32_shmem.c: 137 - 206

## Overview
Attempts to acquire the SeLockMemoryPrivilege Windows user right so that PostgreSQL can use large pages for improved memory management performance.

## Definition


## Detailed Description
This Windows-specific function tries to enable the "Lock pages in memory" privilege for the current PostgreSQL process. This privilege is required to use large pages (huge pages) on Windows, which can significantly improve performance by reducing TLB (Translation Lookaside Buffer) misses for large memory allocations.

The function performs the following sequence of operations:
1. **Token Access**: Opens the current process token with TOKEN_ADJUST_PRIVILEGES and TOKEN_QUERY access
2. **Privilege Lookup**: Uses LookupPrivilegeValue() to get the LUID for SE_LOCK_MEMORY_NAME
3. **Privilege Adjustment**: Calls AdjustTokenPrivileges() to enable the privilege
4. **Error Checking**: Verifies the operation succeeded and handles specific error cases

The function provides detailed error reporting with appropriate PostgreSQL error codes and hints for system administrators.

## Parameters / Member Variables
- : Error reporting level (e.g., ERROR, WARNING, LOG) to use if privilege acquisition fails

## Dependencies
- Functions called/Symbols referenced:
  - OpenProcessToken (Windows API)
  - GetCurrentProcess (Windows API)  
  - LookupPrivilegeValue (Windows API)
  - AdjustTokenPrivileges (Windows API)
  - CloseHandle (Windows API)
  - GetLastError (Windows API)
  - ereport
  - errmsg
  - errdetail
  - errhint
  - errcode
- Called from (representative examples):
  - PGSharedMemoryCreate

## Notes and Other Information
- **Windows-only**: This function exists only in the Windows port (win32_shmem.c)
- **Privilege Requirement**: The Windows user account running PostgreSQL must have the "Lock pages in memory" user right assigned through Local Security Policy
- **Large Pages**: Required for using large pages on Windows, which can improve performance for systems with large shared memory requirements
- **Error Handling**: Provides specific handling for ERROR_NOT_ALL_ASSIGNED, giving users helpful hints about assigning the required user right
- **Security Context**: Operates on the current process token and requires appropriate permissions to modify privileges
- **Resource Cleanup**: Properly closes the process token handle in all code paths
- **Localization**: Error messages are marked for translation to match Windows localization