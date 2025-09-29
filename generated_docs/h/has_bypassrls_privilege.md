# has_bypassrls_privilege

## Location
[src/backend/catalog/aclchk.c:4247-4270](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L4247-L4270)

## Overview
Checks whether a specified role has BYPASSRLS (Bypass Row Level Security) privilege or is a superuser, determining if the role can bypass row-level security policies.

## Definition
```c
bool has_bypassrls_privilege(Oid roleid)
```

## Detailed Description
This function determines if a role has the BYPASSRLS privilege, which allows the role to bypass row-level security (RLS) policies on tables. Row-level security is a feature that restricts which rows a user can see or modify based on policies defined on the table. The function first checks if the role is a superuser (who automatically bypass all security restrictions), then examines the `rolbypassrls` attribute in the pg_authid system catalog to determine if the role has been granted the specific BYPASSRLS privilege.

## Parameters / Member Variables
- `roleid`: The OID of the role whose BYPASSRLS privilege is being checked

## Dependencies
- Functions called/Symbols referenced:
  - [superuser_arg](../s/superuser_arg.md)
  - Form_pg_authid
  - [Acl](../A/Acl.md)
- Called from (representative examples):
  - [CreateRole](../C/CreateRole.md)
  - [AlterRole](../A/AlterRole.md)
  - [RI_Initial_Check](../R/RI_Initial_Check.md)
  - [check_enable_rls](../c/check_enable_rls.md)

## Notes and Other Information
- Located in src/backend/catalog/aclchk.c:4247-4270
- Returns true if the role has BYPASSRLS privilege or is a superuser, false otherwise
- Critical for PostgreSQL's Row Level Security (RLS) enforcement system
- The BYPASSRLS privilege is stored in the `rolbypassrls` field of pg_authid
- Used during role creation/alteration and RLS policy evaluation
- Superusers automatically have this privilege regardless of the rolbypassrls setting
- Essential component of PostgreSQL's fine-grained security model
- Allows privileged roles to see all data regardless of RLS policies

## Simplified Source

```c
bool
has_bypassrls_privilege(Oid roleid)
{
    bool result = false;
    HeapTuple utup;

    // Superusers bypass all permission checking
    if (superuser_arg(roleid))
        return true;

    utup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
    if (HeapTupleIsValid(utup))
    {
        result = ((Form_pg_authid) GETSTRUCT(utup))->rolbypassrls;
        ReleaseSysCache(utup);
    }
    return result;
}
```