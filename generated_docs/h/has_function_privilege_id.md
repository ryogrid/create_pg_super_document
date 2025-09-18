# has_function_privilege_id

## Location
src/backend/utils/adt/acl.c: 3476 - 3503

## Overview
Checks whether the current user has specific privileges on a function identified by its OID.

## Definition


## Detailed Description
This function is a PostgreSQL built-in function that verifies if the current user has the specified privilege on a given function. It takes a function OID and a privilege name as text, then checks the access control list (ACL) to determine if the current user has that privilege on the function. The function assumes the current user context and returns a boolean result or NULL if the function doesn't exist.

## Parameters / Member Variables
-  (PG_GETARG_OID(0)): The OID of the function to check privileges for
-  (PG_GETARG_TEXT_PP(1)): Text representation of the privilege type to check (e.g., 'EXECUTE')

## Dependencies
- Functions called/Symbols referenced:
  - GetUserId(): Gets the current user's OID
  - convert_function_priv_string(): Converts privilege string to AclMode bitmask
  - object_aclcheck_ext(): Performs the actual privilege check against the ACL
  - AclResult: Enum type for ACL check results
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- This is one of the PostgreSQL privilege checking functions available in SQL
- Returns NULL if the specified function doesn't exist (is_missing flag)
- Part of the has_*_privilege family of functions for access control verification
- Located in src/backend/utils/adt/acl.c:3476-3503