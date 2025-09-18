# has_type_privilege_name_id

## Location
[src/backend/utils/adt/acl.c:4456-4485](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L4456-L4485)

## Overview
Checks whether a specified user has a particular privilege on a type, where the username is provided as text and the type is identified by its OID.

## Definition
```c
Datum has_type_privilege_name_id(PG_FUNCTION_ARGS)
```

## Detailed Description
This PostgreSQL function provides an optimized variant of type privilege checking where the type is already known by its Object Identifier (OID), avoiding the need for name-to-OID conversion. This is particularly useful when the calling code already has the type OID available, such as during catalog operations or when working with cached type information. The function takes a username as text, a type OID, and a privilege specification, then performs the privilege check using PostgreSQL's extended access control system that can detect missing objects.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]`: Username as a Name type (the user whose privileges are being checked)
- `PG_FUNCTION_ARGS[1]`: Type OID as Oid (the target type identifier for privilege checking)
- `PG_FUNCTION_ARGS[2]`: Privilege specification as text (e.g., "USAGE")

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME
  - PG_GETARG_OID
  - PG_GETARG_TEXT_PP
  - get_role_oid_or_public
  - [convert_type_priv_string](../c/convert_type_priv_string.md)
  - [object_aclcheck_ext](../o/object_aclcheck_ext.md)
  - PG_RETURN_NULL
  - PG_RETURN_BOOL
- Called from (representative examples):
  - This function is callable from SQL as has_type_privilege()

## Notes and Other Information
- This is a PostgreSQL SQL-callable function that returns a boolean result or NULL
- Uses object_aclcheck_ext instead of object_aclcheck to detect missing types and return NULL appropriately
- Returns true if the user has the specified privilege, false if not, or NULL if the type doesn't exist
- Part of the has_type_privilege family of functions with different parameter combinations
- More efficient than name-based variants when the type OID is already available
- The is_missing flag allows proper NULL handling for non-existent types
- Handles both regular users and the special "public" pseudo-role