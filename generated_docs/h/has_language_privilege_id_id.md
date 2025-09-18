# has_language_privilege_id_id

## Location
[src/backend/utils/adt/acl.c:3736-3764](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L3736-L3764)

## Overview
Checks whether a specific user (identified by role OID) has the specified privileges on a procedural language (identified by language OID).

## Definition


## Detailed Description
This function is a PostgreSQL internal function that performs privilege checking for procedural languages. It takes three arguments: a role OID, a language OID, and a privilege string, then determines whether the specified role has the requested privileges on the given language. The function uses PostgreSQL's standard access control mechanisms through  to perform the actual privilege verification. If the language object is missing from the system catalogs, the function returns NULL to indicate an undefined state.

## Parameters / Member Variables
-  (Oid): The OID of the role/user whose privileges are being checked
-  (Oid): The OID of the procedural language object to check privileges against  
-  (text*): A text string specifying the privilege type to check (e.g., "USAGE")

## Dependencies
- Functions called/Symbols referenced:
  - [convert_language_priv_string](../c/convert_language_priv_string.md): Converts the privilege string to an AclMode bitmask
  - [object_aclcheck_ext](../o/object_aclcheck_ext.md): Performs the actual ACL privilege check against the object
  - AclResult: Enum type for ACL check results
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- This function is part of PostgreSQL's privilege checking infrastructure for procedural languages
- Returns NULL if the specified language does not exist, following PostgreSQL's convention for missing objects
- The function follows the standard PostgreSQL function calling convention using PG_FUNCTION_ARGS
- Located in src/backend/utils/adt/acl.c:3736-3764