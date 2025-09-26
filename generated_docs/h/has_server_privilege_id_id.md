# has_server_privilege_id_id

## Location
[src/backend/utils/adt/acl.c:4138-4166](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L4138-L4166)

## Overview
This function checks user privileges on a foreign server, taking a role ID (OID), server ID (OID), and privilege name (text) as input parameters, with support for handling missing objects.

## Definition
```c
Datum has_server_privilege_id_id(PG_FUNCTION_ARGS)
```

## Detailed Description
The `has_server_privilege_id_id` function is a PostgreSQL built-in function that verifies whether a specified role has particular privileges on a foreign server identified by its OID. Unlike `has_server_privilege_id_name`, this function takes the server OID directly rather than converting from a name. It uses the extended ACL checking mechanism (`object_aclcheck_ext`) which can detect when the target object is missing and return NULL in such cases, following PostgreSQL's convention for privilege-checking functions when objects don't exist.

## Parameters / Member Variables
- `roleid`: OID of the role whose privileges are being checked
- `serverid`: OID of the foreign server to check privileges on
- `priv_type_text`: Text representation of the privilege type to check (e.g., "USAGE")

## Dependencies
- Functions called/Symbols referenced:
  - [convert_server_priv_string](../c/convert_server_priv_string.md)
  - [object_aclcheck_ext](../o/object_aclcheck_ext.md)
  - [AclResult](../A/AclResult.md)
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
This function is part of PostgreSQL's privilege checking system for foreign servers and is optimized for cases where the server OID is already known. The key difference from similar functions is its use of `object_aclcheck_ext` with the `is_missing` parameter, which allows it to return NULL when the specified server doesn't exist, rather than throwing an error. This behavior is consistent with PostgreSQL's has_*_privilege family of functions that return NULL for non-existent objects.