# has_foreign_data_wrapper_privilege_name_name

## Location
[src/backend/utils/adt/acl.c:3196-3221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L3196-L3221)

## Overview
Checks foreign data wrapper privileges for a specific user given the username, FDW name, and privilege name.

## Definition
```c
Datum has_foreign_data_wrapper_privilege_name_name(PG_FUNCTION_ARGS)
```

## Detailed Description
This SQL-callable function determines whether a user (specified by username) has specific privileges on a foreign data wrapper (specified by name). It is part of PostgreSQL foreign data wrapper access control system. The function converts the username to a role OID, converts the FDW name to an OID, converts the privilege string to an internal privilege bitmask, and then performs the actual privilege check using the standard object access control mechanism for foreign data wrappers.

## Parameters / Member Variables
- `PG_GETARG_NAME(0)`: Name of the user whose privileges are being checked
- `PG_GETARG_TEXT_PP(1)`: Text name of the foreign data wrapper to check privileges on
- `PG_GETARG_TEXT_PP(2)`: Text string specifying the privilege type to check

## Dependencies
- Functions called/Symbols referenced:
  - get_role_oid_or_public: Converts username to role OID, handles "public" role
  - [convert_foreign_data_wrapper_name](../c/convert_foreign_data_wrapper_name.md): Converts FDW name to OID
  - [convert_foreign_data_wrapper_priv_string](../c/convert_foreign_data_wrapper_priv_string.md): Converts privilege string to AclMode bitmask
  - [object_aclcheck](../o/object_aclcheck.md): Performs the actual privilege check using ForeignDataWrapperRelationId
  - Name: PostgreSQL name type for usernames
  - AclResult: Enum type for access control results
- Called from (representative examples):
  - This function is typically called from SQL queries using the has_foreign_data_wrapper_privilege() function

## Notes and Other Information
- Returns a boolean Datum: true if the user has the specified privilege, false otherwise
- Part of the has_foreign_data_wrapper_privilege family of functions that provide different parameter combinations
- Uses ForeignDataWrapperRelationId as the object class for privilege checking
- The function performs error checking through the convert_* helper functions
- Located in src/backend/utils/adt/acl.c:3196-3221