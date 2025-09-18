# convert_tablespace_priv_string

## Location
[src/backend/utils/adt/acl.c:4379-4405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L4379-L4405)

## Overview
Converts a text string representation of tablespace privileges into the corresponding AclMode bitmask value for privilege checking operations.

## Definition
```c
static AclMode convert_tablespace_priv_string(text *priv_type_text)
```

## Detailed Description
This function converts human-readable privilege strings into the internal AclMode representation used by PostgreSQL's access control system for tablespaces. It defines a static mapping table that associates tablespace privilege names ("CREATE" and "CREATE WITH GRANT OPTION") with their corresponding ACL bitmask values. The function leverages the generic convert_any_priv_string function to perform the actual conversion, making it consistent with other privilege conversion functions throughout the system.

## Parameters / Member Variables
- `priv_type_text`: A PostgreSQL text type containing the privilege string to convert (e.g., "CREATE", "CREATE WITH GRANT OPTION")

## Dependencies
- Functions called/Symbols referenced:
  - [convert_any_priv_string](convert_any_priv_string.md)
  - ACL_CREATE
  - ACL_GRANT_OPTION_FOR
- Called from (representative examples):
  - [has_tablespace_privilege_name_name](../h/has_tablespace_privilege_name_name.md)
  - [has_tablespace_privilege_name](../h/has_tablespace_privilege_name.md)
  - [has_tablespace_privilege_name_id](../h/has_tablespace_privilege_name_id.md)
  - [has_tablespace_privilege_id](../h/has_tablespace_privilege_id.md)
  - [has_tablespace_privilege_id_name](../h/has_tablespace_privilege_id_name.md)
  - [has_tablespace_privilege_id_id](../h/has_tablespace_privilege_id_id.md)

## Notes and Other Information
- This is a static function, only accessible within the acl.c compilation unit
- Tablespaces only support the CREATE privilege, unlike other database objects that may support multiple privilege types
- The function uses a static privilege mapping table that is initialized once and reused
- Part of the has_tablespace_privilege family of functions
- Follows the same pattern as other privilege conversion functions in PostgreSQL