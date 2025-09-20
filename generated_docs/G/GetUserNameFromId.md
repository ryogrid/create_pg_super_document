# GetUserNameFromId

## Location
[src/backend/utils/init/miscinit.c:1034-1070](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L1034-L1070)

## Overview
GetUserNameFromId retrieves the user name (role name) from a given user OID, with optional error handling for nonexistent roles.

## Definition

```c
typedef struct SerializedClientConnectionInfo
{
	int32		authn_id_len;	/* strlen(authn_id), or -1 if NULL */
	UserAuth	auth_method;
} SerializedClientConnectionInfo;
```
## Detailed Description
This function looks up a user name from the PostgreSQL system catalog using the provided role OID. It searches the pg_authid system catalog to find the role entry and extracts the role name. The function provides flexible error handling: when noerr is true, it returns NULL for nonexistent roles; when noerr is false, it raises an ERROR for invalid role OIDs. The function uses PostgreSQL's system cache mechanism for efficient lookups and returns a palloc'd copy of the role name that the caller must free.

## Parameters / Member Variables
- `authn_id_len`: The Oid of the role/user to look up
- `auth_method`: Boolean flag controlling error behavior - if true, returns NULL for nonexistent roles; if false, raises an error
## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - AUTHOID (catalog cache identifier)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (Oid to Datum conversion)
  - HeapTupleIsValid (tuple validation)
  - ereport (error reporting)
  - Form_pg_authid (pg_authid catalog structure)
  - GETSTRUCT (tuple structure extraction)
  - NameStr (Name to C string conversion)
  - [pstrdup](../p/pstrdup.md) (palloc'd string duplication)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
- Called from (representative examples):
  - [getObjectDescription](../g/getObjectDescription.md) (in catalog object descriptions)
  - [current_user](../c/current_user.md) and session_user (in name functions)
  - [check_role_membership_authorization](../c/check_role_membership_authorization.md) (in user commands)
  - [AddRoleMems](../A/AddRoleMems.md) and DelRoleMems (in role membership management)

## Notes and Other Information
- Returns a palloc'd string that must be freed by the caller
- Uses system cache for efficient role name lookups
- Critical function for role-based access control and user management
- Used extensively throughout PostgreSQL for converting role OIDs to names
- Essential for error messages, logging, and user-facing output that displays role names
- The noerr parameter allows for graceful handling of invalid role references