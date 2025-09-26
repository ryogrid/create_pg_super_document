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
- : Handle to the Windows access token from which to determine privileges to delete

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