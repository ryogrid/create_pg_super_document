# system_user

## Location
[src/backend/utils/init/miscinit.c:944-965](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L944-L965)

## Overview
The system_user function is a PostgreSQL SQL function that returns the system user name associated with the current database session.

## Definition

```c
Datum
system_user(PG_FUNCTION_ARGS)
```
## Detailed Description
The system_user function implements the SQL SYSTEM_USER function in PostgreSQL. It retrieves the system user name through the GetSystemUser() function and returns it as a PostgreSQL text datum. If no system user is available, the function returns NULL. This function is part of PostgreSQL's security and session management infrastructure, providing information about the authenticated system user for the current session.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [GetSystemUser](../G/GetSystemUser.md)
  - PG_RETURN_DATUM
  - CStringGetTextDatum
  - PG_RETURN_NULL
- Called from (representative examples):
  - [parse_ident_line](../p/parse_ident_line.md) (in HBA authentication)
  - [check_ident_usermap](../c/check_ident_usermap.md) (in identity mapping)
  - [InitializeSystemUser](../I/InitializeSystemUser.md) (during system user initialization)

## Notes and Other Information
- This function is primarily used in PostgreSQL's authentication and authorization system
- It's commonly referenced in HBA (Host-Based Authentication) processing for identity mapping
- The function may return NULL if no system user information is available
- It serves as the backend implementation for the SQL SYSTEM_USER function that can be called from SQL queries

## Simplified Source

```c
// Simplified version of system_user
Datum system_user(PG_FUNCTION_ARGS) {
    // Get the system user name from the authentication system
    const char *sysuser = GetSystemUser();

    // Return the system user as text, or NULL if unavailable
    if (sysuser)
        PG_RETURN_DATUM(CStringGetTextDatum(sysuser));
    else
        PG_RETURN_NULL();
}
```

Key simplifications made:
- Added explanatory comments for each logical step
- The function is already quite simple, so minimal changes were needed
- Preserved the essential null-checking logic
- Focused on the core functionality: retrieve system user and return as PostgreSQL datum