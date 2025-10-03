# GetSystemUser

## Location
[src/backend/utils/init/miscinit.c:581-590](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L581-L590)

## Overview
Returns the system user string representing the authenticated identity, formatted as "auth_method:authn_id" to uniquely identify how and as whom the current session was authenticated.

## Definition

```c
const char *
GetSystemUser(void)
```
## Detailed Description
GetSystemUser is a simple accessor function that returns the globally stored system user identifier. This identifier is a formatted string that combines the authentication method used (such as "md5", "trust", "peer", etc.) with the authenticated identity (username or other identifier provided by the auth method). 

The system user is initialized once per session by InitializeSystemUser() during the authentication process. This provides a complete audit trail of both the authentication method used and the identity that was authenticated, which is valuable for logging, security auditing, and session tracking.

The returned string remains constant throughout the session and is stored in long-lived memory (TopMemoryContext) to ensure it persists for the entire connection lifetime.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - SystemUser (global static variable access)
- Called from (representative examples):
  - [system_user](../s/system_user.md) (function in miscinit.c:946)
  - External functions via miscadmin.h header inclusion

## Notes and Other Information
- Returns a pointer to static memory that should not be modified or freed by callers
- The SystemUser variable is set exactly once during session initialization via InitializeSystemUser()
- The format is always "auth_method:authn_id" (e.g., "md5:username" or "peer:username")
- This function provides access to authentication details that are distinct from the current effective user ID or session user ID
- Used primarily for auditing and logging purposes to track the original authentication context
- The function is declared in miscadmin.h and available throughout the PostgreSQL backend

## Simplified Source

```c
// Simplified version of GetSystemUser
const char *GetSystemUser(void) {
    // Return the global system user string (auth_method:authn_id)
    return SystemUser;
}
```

Key simplifications made:
- Added explanatory comment describing what the function returns
- This function is already very simple, being just a direct accessor
- Preserved the const char* return type for the static string
- Maintained the direct access to the global SystemUser variable