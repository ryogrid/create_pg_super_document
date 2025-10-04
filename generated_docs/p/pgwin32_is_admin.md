# pgwin32_is_admin

## Location
[src/port/win32security.c:49-119](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/win32security.c#L49-L119)

## Overview
A Windows-specific function that determines whether the current user has administrative privileges by checking membership in the Administrators and PowerUsers groups.

## Definition

```c
int
pgwin32_is_admin(void)
```
## Detailed Description
The  function is a security utility specifically designed for Windows platforms to check if the current user possesses administrative privileges. It performs this check by examining membership in two critical Windows security groups: the Administrators group and the PowerUsers group (a legacy group from older Windows versions that provided elevated privileges).

The function uses Windows Security Identifier (SID) APIs to create SIDs for these groups and then checks the current process token's membership against these SIDs. The implementation is robust and includes proper error handling, though it uses  to terminate the process if critical security operations fail, indicating this function is typically called during startup when failure recovery is not feasible.

This function is particularly important for PostgreSQL's Windows port as it helps determine appropriate security contexts and access levels during initialization.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [log_error](../l/log_error.md) (for error reporting)
  - AllocateAndInitializeSid (Windows API)
  - CheckTokenMembership (Windows API)
  - FreeSid (Windows API)
  - GetLastError (Windows API)
  - exit (system call)
- Called from (representative examples):
  - [check_root](../c/check_root.md) (in src/backend/main/main.c)
  - Referenced in src/include/port/win32_port.h

## Notes and Other Information
- This function is Windows-specific and located in the port-specific directory structure
- The function cannot use  because it's called too early during startup, hence it uses the  macro instead
- Returns non-zero (1) if the user has administrative privileges, zero (0) otherwise
- The function checks both Administrators and PowerUsers groups for backward compatibility with older Windows versions
- Uses  for fatal errors, making it suitable for startup-time security checks where failure should terminate the process
- Critical for PostgreSQL's security model on Windows platforms
- The PowerUsers group check provides compatibility with legacy Windows systems where this group had administrative-like privileges

## Simplified Source

```c
int
pgwin32_is_admin(void)
{
    PSID AdministratorsSid;
    PSID PowerUsersSid;
    SID_IDENTIFIER_AUTHORITY NtAuthority = {SECURITY_NT_AUTHORITY};
    BOOL IsAdministrators;
    BOOL IsPowerUsers;

    // Create SID for Administrators group
    if (!AllocateAndInitializeSid(&NtAuthority, 2,
                                  SECURITY_BUILTIN_DOMAIN_RID,
                                  DOMAIN_ALIAS_RID_ADMINS, 0, 0, 0, 0, 0,
                                  0, &AdministratorsSid)) {
        log_error(_("could not get SID for Administrators group: error code %lu\n"),
                  GetLastError());
        exit(1);
    }

    // Create SID for PowerUsers group
    if (!AllocateAndInitializeSid(&NtAuthority, 2,
                                  SECURITY_BUILTIN_DOMAIN_RID,
                                  DOMAIN_ALIAS_RID_POWER_USERS, 0, 0, 0, 0, 0,
                                  0, &PowerUsersSid)) {
        log_error(_("could not get SID for PowerUsers group: error code %lu\n"),
                  GetLastError());
        exit(1);
    }

    // Check token membership in both groups
    if (!CheckTokenMembership(NULL, AdministratorsSid, &IsAdministrators) ||
        !CheckTokenMembership(NULL, PowerUsersSid, &IsPowerUsers)) {
        log_error(_("could not check access token membership: error code %lu\n"),
                  GetLastError());
        exit(1);
    }

    // Clean up SIDs
    FreeSid(AdministratorsSid);
    FreeSid(PowerUsersSid);

    // Return 1 if user is in either administrative group
    return (IsAdministrators || IsPowerUsers) ? 1 : 0;
}
```