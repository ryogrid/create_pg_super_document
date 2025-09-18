# has_server_privilege_id_name

## Location
[src/backend/utils/adt/acl.c:4115-4137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L4115-L4137)

## Overview
This function checks user privileges on a foreign server, taking a role ID (OID), server name (text), and privilege name (text) as input parameters.

## Definition
```c
Datum has_server_privilege_id_name(PG_FUNCTION_ARGS)
```

## Detailed Description
The `has_server_privilege_id_name` function is a PostgreSQL built-in function that verifies whether a specified role has particular privileges on a named foreign server. It accepts three arguments: a role OID, a text representation of the server name, and a text representation of the privilege type. The function converts the server name to its corresponding OID and the privilege string to an access control mode bitmask, then performs an ACL (Access Control List) check using the standard PostgreSQL privilege checking mechanism. It returns a boolean value indicating whether the specified role has the requested privileges on the target server.

## Parameters / Member Variables
- `roleid`: OID of the role whose privileges are being checked
- `servername`: Text representation of the foreign server name
- `priv_type_text`: Text representation of the privilege type to check (e.g., "USAGE")

## Dependencies
- Functions called/Symbols referenced:
  - [convert_server_name](../c/convert_server_name.md)
  - [convert_server_priv_string](../c/convert_server_priv_string.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - AclResult
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
This function is part of PostgreSQL's privilege checking system for foreign servers. It serves as one of the has_*_privilege family of functions that allow checking access permissions programmatically. The function follows the standard PostgreSQL function calling convention using PG_FUNCTION_ARGS and returns a Datum. The actual privilege checking is delegated to the generic object_aclcheck function with ForeignServerRelationId as the object class.