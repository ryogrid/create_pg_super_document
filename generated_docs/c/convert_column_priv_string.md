# convert_column_priv_string

## Location
[src/backend/utils/adt/acl.c:2956-2989](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L2956-L2989)

## Overview
Converts a text string representing column privileges into an AclMode bitmask value, specifically handling column-level access control permissions.

## Definition

```c
static AclMode
convert_column_priv_string(text *priv_type_text)
```
## Detailed Description
This static function parses a privilege string for column-level permissions and converts it to the corresponding AclMode bitmask. It uses a predefined mapping table (column_priv_map) that associates privilege name strings with their corresponding ACL constants. The function supports the standard column privileges: SELECT, INSERT, UPDATE, and REFERENCES, along with their "WITH GRANT OPTION" variants. The actual conversion logic is delegated to the generic convert_any_priv_string function.

## Parameters / Member Variables
- `*priv_type_text`: A PostgreSQL text object containing the privilege string to be converted (e.g., "SELECT", "INSERT WITH GRANT OPTION")
## Dependencies
- Functions called/Symbols referenced:
  - [convert_any_priv_string](convert_any_priv_string.md)
  - priv_map (struct type)
  - ACL_SELECT
  - ACL_INSERT 
  - ACL_UPDATE
  - ACL_REFERENCES
  - ACL_GRANT_OPTION_FOR (macro)
- Called from (representative examples):
  - [has_any_column_privilege_name_name](../h/has_any_column_privilege_name_name.md)
  - [has_any_column_privilege_name](../h/has_any_column_privilege_name.md)
  - [has_column_privilege_name_name_name](../h/has_column_privilege_name_name_name.md)
  - [has_column_privilege_id_attnum](../h/has_column_privilege_id_attnum.md)

## Notes and Other Information
- This is a static function, only accessible within the acl.c file
- Uses a local privilege mapping table that defines the supported column privilege types
- Column privileges are more restrictive than table privileges - only SELECT, INSERT, UPDATE, and REFERENCES are supported
- The function leverages the generic convert_any_priv_string utility function for the actual string parsing and conversion logic
- Located in src/backend/utils/adt/acl.c:2956-2989

## Simplified Source

```c
static AclMode convert_column_priv_string(text *priv_type_text) {
    // Define mapping of privilege strings to ACL modes
    static const priv_map column_priv_map[] = {
        {"SELECT", ACL_SELECT},
        {"SELECT WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_SELECT)},
        {"INSERT", ACL_INSERT},
        {"INSERT WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_INSERT)},
        {"UPDATE", ACL_UPDATE},
        {"UPDATE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_UPDATE)},
        {"REFERENCES", ACL_REFERENCES},
        {"REFERENCES WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_REFERENCES)},
        {NULL, 0}
    };

    // Use generic conversion function with column privilege mapping
    return convert_any_priv_string(priv_type_text, column_priv_map);
}
```