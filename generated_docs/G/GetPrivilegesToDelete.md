# GetPrivilegesToDelete

## Location
[src/bin/pg_ctl/pg_ctl.c:1896-1952](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L1896-L1952)

## Overview
Retrieves a list of Windows privileges to remove from an access token, preserving only essential privileges needed for PostgreSQL operation while enhancing security.

## Definition

```c
static PTOKEN_PRIVILEGES
GetPrivilegesToDelete(HANDLE hToken)
```
## Detailed Description
This function creates a list of privileges that should be removed from a Windows access token to implement privilege restriction. It first retrieves all privileges from the specified token, then removes two critical privileges that must be preserved: SeLockMemoryPrivilege (needed for large pages support) and SeChangeNotifyPrivilege (enabled by default and required for file traversal). The function uses LookupPrivilegeValue to get the LUIDs for these preserved privileges, then iterates through the token's privilege list to filter them out. The resulting structure contains all other privileges that can be safely removed to minimize the attack surface.

## Parameters / Member Variables
- `hToken`: Handle to the Windows access token from which to determine privileges to delete
## Dependencies
- Functions called/Symbols referenced:
  - LookupPrivilegeValue (Windows API)
  - GetTokenInformation (Windows API)
  - [pg_malloc_extended](../p/pg_malloc_extended.md)
  - [write_stderr](../w/write_stderr.md)
  - GetLastError (Windows API)
  - memcmp
  - free
  - MCXT_ALLOC_NO_OOM
- Called from (representative examples):
  - [CreateRestrictedProcess](../C/CreateRestrictedProcess.md)

## Notes and Other Information
- Returns a PTOKEN_PRIVILEGES structure containing privileges to remove, or NULL on failure
- Preserves SeLockMemoryPrivilege to maintain large pages functionality
- Preserves SeChangeNotifyPrivilege which is required for basic file system operations
- Uses pg_malloc_extended with MCXT_ALLOC_NO_OOM for memory allocation
- The caller is responsible for freeing the returned structure
- Error handling includes detailed Windows error codes for debugging
- The function removes preserved privileges from the list by shifting array elements
- This is a key component of PostgreSQL's Windows security hardening strategy

## Simplified Source

```c
static PTOKEN_PRIVILEGES
GetPrivilegesToDelete(HANDLE hToken)
{
    DWORD length;
    PTOKEN_PRIVILEGES tokenPrivs;
    LUID luidLockPages, luidChangeNotify;

    // Get LUIDs for privileges we want to preserve
    if (!LookupPrivilegeValue(NULL, SE_LOCK_MEMORY_NAME, &luidLockPages) ||
        !LookupPrivilegeValue(NULL, SE_CHANGE_NOTIFY_NAME, &luidChangeNotify)) {
        write_stderr(_("%s: could not get LUIDs for privileges: error code %lu\n"),
                     progname, (unsigned long) GetLastError());
        return NULL;
    }

    // Get size of token privileges information
    if (!GetTokenInformation(hToken, TokenPrivileges, NULL, 0, &length) &&
        GetLastError() != ERROR_INSUFFICIENT_BUFFER) {
        write_stderr(_("%s: could not get token information: error code %lu\n"),
                     progname, (unsigned long) GetLastError());
        return NULL;
    }

    // Allocate memory for privilege information
    tokenPrivs = (PTOKEN_PRIVILEGES) pg_malloc_extended(length, MCXT_ALLOC_NO_OOM);
    if (tokenPrivs == NULL) {
        write_stderr(_("%s: out of memory\n"), progname);
        return NULL;
    }

    // Get actual token privileges information
    if (!GetTokenInformation(hToken, TokenPrivileges, tokenPrivs, length, &length)) {
        write_stderr(_("%s: could not get token information: error code %lu\n"),
                     progname, (unsigned long) GetLastError());
        free(tokenPrivs);
        return NULL;
    }

    // Remove preserved privileges from the list
    for (int i = 0; i < tokenPrivs->PrivilegeCount; i++) {
        // Check if this privilege should be preserved
        if (memcmp(&tokenPrivs->Privileges[i].Luid, &luidLockPages, sizeof(LUID)) == 0 ||
            memcmp(&tokenPrivs->Privileges[i].Luid, &luidChangeNotify, sizeof(LUID)) == 0) {
            // Shift remaining privileges down to remove this one
            for (int j = i; j < tokenPrivs->PrivilegeCount - 1; j++)
                tokenPrivs->Privileges[j] = tokenPrivs->Privileges[j + 1];
            tokenPrivs->PrivilegeCount--;
            i--; // Recheck current position since we shifted elements
        }
    }

    return tokenPrivs;
}
```