# check_role

## Location
[src/backend/commands/variable.c:921-1013](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/variable.c#L921-L1013)

## Overview
The `check_role` function validates and prepares role changes for the SET ROLE SQL command, performing authentication checks and privilege verification.

## Definition
```c
bool check_role(char **newval, void **extra, GucSource source)
```

## Detailed Description
This function is a GUC (Grand Unified Configuration) check hook that validates role changes initiated by the SET ROLE command. It implements the SQL specification requirement that "SET ROLE NONE" unsets the current role by translating "none" to InvalidOid. The function performs several key operations:

1. **Role Resolution**: Converts role names to OIDs, with special handling for "none"
2. **Privilege Verification**: Ensures the session user has permission to assume the target role
3. **Catalog Lookups**: Searches the pg_authid system catalog to validate role existence
4. **Parallel Worker Handling**: Special logic for parallel worker initialization
5. **Extra Data Preparation**: Sets up auxiliary data structure for the subsequent assign operation

The function supports test mode (PGC_S_TEST) where it reports issues as notices rather than hard errors, allowing configuration validation without side effects.

## Parameters / Member Variables
- `newval`: Pointer to the role name string being set; may be modified by the function
- `extra`: Output parameter for additional data structure passed to assign_role function
- `source`: The source of the configuration change (command line, config file, SET command, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentRoleId](../G/GetCurrentRoleId.md)
  - [IsTransactionState](../I/IsTransactionState.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [GetSessionUserId](../G/GetSessionUserId.md)
  - [member_can_set_role](../m/member_can_set_role.md)
  - [guc_malloc](../g/guc_malloc.md)
  - GUC_check_errmsg
  - [GUC_check_errcode](../G/GUC_check_errcode.md)
- Called from (representative examples):
  - [check_hba](check_hba.md) (in authentication context)
  - [check_ident_usermap](check_ident_usermap.md) (in identity mapping)
  - GUC system framework

## Notes and Other Information
- Implements SQL standard requirement for "SET ROLE NONE" semantics
- Cannot be used to set roles in postgresql.conf due to transaction state requirements
- Parallel workers use special logic to copy leader state without catalog verification
- Test mode allows configuration validation with reduced error severity
- Integrates with PostgreSQL's role-based access control system
- Part of the GUC hook mechanism for dynamic configuration validation

## Simplified Source

```c
bool
check_role(char **newval, void **extra, GucSource source)
{
    HeapTuple roleTup;
    Oid roleid;
    bool is_superuser;
    role_auth_extra *myextra;
    Form_pg_authid roleform;

    // Handle "SET ROLE NONE" - unset current role
    if (strcmp(*newval, "none") == 0)
    {
        roleid = InvalidOid;
        is_superuser = false;
    }
    // Special case for parallel worker initialization
    else if (InitializingParallelWorker)
    {
        roleid = GetCurrentRoleId();
        is_superuser = current_role_is_superuser;
    }
    else
    {
        // Need transaction state for catalog lookups
        if (!IsTransactionState())
            return false;

        // Look up the role in system catalog
        roleTup = SearchSysCache1(AUTHNAME, PointerGetDatum(*newval));
        if (!HeapTupleIsValid(roleTup))
        {
            if (source == PGC_S_TEST)
            {
                ereport(NOTICE, (errcode(ERRCODE_UNDEFINED_OBJECT),
                                errmsg("role \"%s\" does not exist", *newval)));
                return true;
            }
            GUC_check_errmsg("role \"%s\" does not exist", *newval);
            return false;
        }

        roleform = (Form_pg_authid) GETSTRUCT(roleTup);
        roleid = roleform->oid;
        is_superuser = roleform->rolsuper;

        ReleaseSysCache(roleTup);

        // Check if session user can assume this role
        if (!member_can_set_role(GetSessionUserId(), roleid))
        {
            if (source == PGC_S_TEST)
            {
                ereport(NOTICE, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                                errmsg("permission will be denied to set role \"%s\"", *newval)));
                return true;
            }
            GUC_check_errcode(ERRCODE_INSUFFICIENT_PRIVILEGE);
            GUC_check_errmsg("permission denied to set role \"%s\"", *newval);
            return false;
        }
    }

    // Set up extra data for assign_role function
    myextra = (role_auth_extra *) guc_malloc(LOG, sizeof(role_auth_extra));
    if (!myextra)
        return false;
    myextra->roleid = roleid;
    myextra->is_superuser = is_superuser;
    *extra = (void *) myextra;

    return true;
}
```