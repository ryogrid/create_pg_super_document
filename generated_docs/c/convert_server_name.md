# convert_server_name

## Location
src/backend/utils/adt/acl.c: 4167 - 4178

## Overview
This static helper function converts a text representation of a foreign server name to its corresponding OID for use in privilege checking operations.

## Definition
```c
static Oid convert_server_name(text *servername)
```

## Detailed Description
The `convert_server_name` function is a support routine for the has_server_privilege family of functions. It takes a PostgreSQL text object containing a server name and converts it to the corresponding foreign server OID. The function performs the conversion by first extracting a C-style string from the text object, then using PostgreSQL's foreign data wrapper system function `get_foreign_server_oid` to look up the server by name. The second parameter (false) to `get_foreign_server_oid` indicates that the function should raise an error if the server is not found, rather than returning InvalidOid.

## Parameters / Member Variables
- `servername`: PostgreSQL text object containing the name of the foreign server to look up

## Dependencies
- Functions called/Symbols referenced:
  - text_to_cstring
  - [get_foreign_server_oid](../g/get_foreign_server_oid.md)
- Called from (representative examples):
  - [has_server_privilege_name_name](../h/has_server_privilege_name_name.md)
  - [has_server_privilege_name](../h/has_server_privilege_name.md)
  - [has_server_privilege_id_name](../h/has_server_privilege_id_name.md)

## Notes and Other Information
This function is declared static, making it internal to the acl.c compilation unit. It serves as a common utility for server name-to-OID conversion within the server privilege checking functions. The function will throw an error if the specified server name does not exist, which is appropriate behavior for privilege checking operations where operating on non-existent objects should fail rather than return a default result.