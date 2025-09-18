# convert_server_priv_string

## Location
src/backend/utils/adt/acl.c: 4179 - 4206

## Overview
This static helper function converts a text representation of server privilege names into their corresponding AclMode bitmask values for foreign server access control.

## Definition
```c
static AclMode convert_server_priv_string(text *priv_type_text)
```

## Detailed Description
The `convert_server_priv_string` function is a support routine that converts textual privilege names specific to foreign servers into their corresponding ACL (Access Control List) mode values. It defines a static mapping table (`server_priv_map`) that associates privilege name strings with their AclMode bit values. For foreign servers, the supported privileges are "USAGE" and "USAGE WITH GRANT OPTION". The function delegates the actual conversion work to the generic `convert_any_priv_string` function, passing the server-specific privilege mapping table.

## Parameters / Member Variables
- `priv_type_text`: PostgreSQL text object containing comma-separated privilege names to convert

## Dependencies
- Functions called/Symbols referenced:
  - convert_any_priv_string
  - priv_map (local structure type)
  - ACL_USAGE
  - ACL_GRANT_OPTION_FOR
- Called from (representative examples):
  - has_server_privilege_name_name
  - has_server_privilege_name
  - has_server_privilege_name_id
  - has_server_privilege_id
  - has_server_privilege_id_name
  - has_server_privilege_id_id

## Notes and Other Information
This function is declared static, making it internal to the acl.c compilation unit. The privilege mapping is specific to foreign servers, which only support USAGE privileges (unlike tables or functions which support multiple privilege types). The static `server_priv_map` array is null-terminated and follows the standard pattern used throughout PostgreSQL's privilege system. The function supports both basic privileges and grant option variants, indicated by the "WITH GRANT OPTION" suffix in the privilege string.