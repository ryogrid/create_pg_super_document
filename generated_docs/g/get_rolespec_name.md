# get_rolespec_name

## Location
[src/backend/utils/adt/acl.c:5556-5577](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L5556-L5577)

## Overview
Retrieves and returns a palloc'ed copy of the role name corresponding to a given RoleSpec structure.

## Definition

```c
char *
get_rolespec_name(const RoleSpec *role)
```
## Detailed Description
This function takes a RoleSpec pointer as input and returns the actual role name as a dynamically allocated string. It serves as a utility function to extract the role name from PostgreSQL's internal RoleSpec representation. The function first retrieves the role tuple using get_rolespec_tuple(), extracts the role name from the pg_authid system catalog entry, creates a palloc'ed copy of the name, and properly releases the system cache entry.

The returned string is allocated using pstrdup() and must be freed by the caller when no longer needed.

## Parameters / Member Variables
- `role`: A pointer to a RoleSpec structure that specifies which role's name to retrieve

## Dependencies
- Functions called/Symbols referenced:
  - [get_rolespec_tuple](get_rolespec_tuple.md)
  - Form_pg_authid (struct type)
  - [pstrdup](../p/pstrdup.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - GETSTRUCT
  - NameStr
- Called from (representative examples):
  - [AddRoleMems](../A/AddRoleMems.md) (in src/backend/commands/user.c)
  - [DelRoleMems](../D/DelRoleMems.md) (in src/backend/commands/user.c)

## Notes and Other Information
- The function assumes the RoleSpec is valid and will find a corresponding role
- Memory management: The caller is responsible for freeing the returned string
- Uses the PostgreSQL system catalog pg_authid to look up role information
- Proper cache management is handled internally with ReleaseSysCache()
- Located in src/backend/utils/adt/acl.c:5556-5577

## Simplified Source

```c
char *get_rolespec_name(const RoleSpec *role) {
    HeapTuple tp;
    Form_pg_authid authForm;
    char *rolename;

    // Get the role tuple from system catalog
    tp = get_rolespec_tuple(role);

    // Extract role name from the tuple
    authForm = (Form_pg_authid) GETSTRUCT(tp);
    rolename = pstrdup(NameStr(authForm->rolname));

    // Release system cache entry
    ReleaseSysCache(tp);

    return rolename;
}
```