# has_foreign_data_wrapper_privilege_name

## Location
[src/backend/utils/adt/acl.c:3222-3245](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L3222-L3245)

## Overview
Checks if the current user has a specified privilege on a foreign data wrapper identified by name.

## Definition
```c
Datum has_foreign_data_wrapper_privilege_name(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL SQL-callable function that verifies whether the current user has a specific privilege on a foreign data wrapper. It takes two text arguments: the name of the foreign data wrapper and the privilege type to check. The function uses the current session's user ID (obtained via GetUserId()) to perform the privilege check. It converts the text-based foreign data wrapper name to an OID, converts the privilege string to an internal AclMode representation, and then uses the general object access control checking mechanism to determine if the privilege is granted.

## Parameters / Member Variables
- `fdwname` (text): The name of the foreign data wrapper to check privileges for
- `priv_type_text` (text): The privilege type to check (e.g., 'USAGE')

## Dependencies
- Functions called/Symbols referenced:
  - [GetUserId](../G/GetUserId.md) (to get current user's OID)
  - [convert_foreign_data_wrapper_name](../c/convert_foreign_data_wrapper_name.md) (converts text name to OID)
  - [convert_foreign_data_wrapper_priv_string](../c/convert_foreign_data_wrapper_priv_string.md) (converts privilege string to AclMode)
  - [object_aclcheck](../o/object_aclcheck.md) (performs the actual privilege check)
  - PG_GETARG_TEXT_PP (macro to extract text arguments)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This function assumes the current user (GetUserId()) as the subject of the privilege check
- Returns true if the privilege check succeeds (ACLCHECK_OK), false otherwise
- Part of PostgreSQL's access control system for foreign data wrappers
- Located in src/backend/utils/adt/acl.c:3222-3245

## Simplified Source

```c
Datum
has_foreign_data_wrapper_privilege_name(PG_FUNCTION_ARGS)
{
    text *fdw_name = PG_GETARG_TEXT_PP(0);
    text *privilege_text = PG_GETARG_TEXT_PP(1);

    // Use current user's OID
    Oid role_oid = GetUserId();

    // Convert FDW name to OID
    Oid fdw_oid = convert_foreign_data_wrapper_name(fdw_name);

    // Convert privilege string to internal mode
    AclMode privilege_mode = convert_foreign_data_wrapper_priv_string(privilege_text);

    // Check privilege on foreign data wrapper
    AclResult result = object_aclcheck(ForeignDataWrapperRelationId, fdw_oid,
                                     role_oid, privilege_mode);

    PG_RETURN_BOOL(result == ACLCHECK_OK);
}
```