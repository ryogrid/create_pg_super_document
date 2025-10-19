# EnableLockPagesPrivilege

## Location
[src/backend/port/win32_shmem.c:137-206](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/win32_shmem.c#L137-L206)

## Overview
Attempts to acquire the SeLockMemoryPrivilege Windows user right so that PostgreSQL can use large pages for improved memory management performance.

## Definition

```c
static bool
EnableLockPagesPrivilege(int elevel)
```
## Detailed Description
This Windows-specific function tries to enable the "Lock pages in memory" privilege for the current PostgreSQL process. This privilege is required to use large pages (huge pages) on Windows, which can significantly improve performance by reducing TLB (Translation Lookaside Buffer) misses for large memory allocations.

The function performs the following sequence of operations:
1. **Token Access**: Opens the current process token with TOKEN_ADJUST_PRIVILEGES and TOKEN_QUERY access
2. **Privilege Lookup**: Uses LookupPrivilegeValue() to get the LUID for SE_LOCK_MEMORY_NAME
3. **Privilege Adjustment**: Calls AdjustTokenPrivileges() to enable the privilege
4. **Error Checking**: Verifies the operation succeeded and handles specific error cases

The function provides detailed error reporting with appropriate PostgreSQL error codes and hints for system administrators.

## Parameters / Member Variables
- `elevel`: Error reporting level (e.g., ERROR, WARNING, LOG) to use if privilege acquisition fails
## Dependencies
- Functions called/Symbols referenced:
  - OpenProcessToken (Windows API)
  - GetCurrentProcess (Windows API)  
  - LookupPrivilegeValue (Windows API)
  - AdjustTokenPrivileges (Windows API)
  - CloseHandle (Windows API)
  - GetLastError (Windows API)
  - ereport
  - [errmsg](../e/errmsg.md)
  - [errdetail](../e/errdetail.md)
  - [errhint](../e/errhint.md)
  - [errcode](../e/errcode.md)
- Called from (representative examples):
  - [PGSharedMemoryCreate](../P/PGSharedMemoryCreate.md)

## Notes and Other Information
- **Windows-only**: This function exists only in the Windows port (win32_shmem.c)
- **Privilege Requirement**: The Windows user account running PostgreSQL must have the "Lock pages in memory" user right assigned through Local Security Policy
- **Large Pages**: Required for using large pages on Windows, which can improve performance for systems with large shared memory requirements
- **Error Handling**: Provides specific handling for ERROR_NOT_ALL_ASSIGNED, giving users helpful hints about assigning the required user right
- **Security Context**: Operates on the current process token and requires appropriate permissions to modify privileges
- **Resource Cleanup**: Properly closes the process token handle in all code paths
- **Localization**: Error messages are marked for translation to match Windows localization

## Simplified Source

```c
static bool
EnableLockPagesPrivilege(int elevel)
{
    HANDLE hToken;
    TOKEN_PRIVILEGES tp;
    LUID luid;

    // Open current process token for privilege adjustment
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_ADJUST_PRIVILEGES | TOKEN_QUERY, &hToken))
    {
        ereport(elevel, (errmsg("could not enable user right \"Lock pages in memory\": error code %lu", GetLastError())));
        return FALSE;
    }

    // Look up the privilege value for locking memory
    if (!LookupPrivilegeValue(NULL, SE_LOCK_MEMORY_NAME, &luid))
    {
        ereport(elevel, (errmsg("could not enable user right \"Lock pages in memory\": error code %lu", GetLastError())));
        CloseHandle(hToken);
        return FALSE;
    }

    // Set up privilege structure
    tp.PrivilegeCount = 1;
    tp.Privileges[0].Luid = luid;
    tp.Privileges[0].Attributes = SE_PRIVILEGE_ENABLED;

    // Enable the privilege
    if (!AdjustTokenPrivileges(hToken, FALSE, &tp, 0, NULL, NULL))
    {
        ereport(elevel, (errmsg("could not enable user right \"Lock pages in memory\": error code %lu", GetLastError())));
        CloseHandle(hToken);
        return FALSE;
    }

    // Check for specific privilege assignment errors
    if (GetLastError() == ERROR_NOT_ALL_ASSIGNED)
    {
        ereport(elevel, (errmsg("could not enable user right \"Lock pages in memory\""),
                        errhint("Assign user right \"Lock pages in memory\" to the Windows user account which runs PostgreSQL.")));
        CloseHandle(hToken);
        return FALSE;
    }

    CloseHandle(hToken);
    return TRUE;
}
```