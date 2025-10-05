# convert_table_priv_string

## Location
[src/backend/utils/adt/acl.c:2064-2107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L2064-L2107)

## Overview
Converts a text string representation of table privileges to PostgreSQL's internal AclMode bitmask representation.

## Definition
```c
static AclMode convert_table_priv_string(text *priv_type_text)
```

## Detailed Description
This static helper function is part of PostgreSQL's privilege checking infrastructure for tables. It takes a text string specifying table privileges (such as "SELECT", "INSERT", "UPDATE", etc.) and converts it to the corresponding AclMode bitmask that PostgreSQL uses internally for privilege representation and checking.

The function maintains a comprehensive mapping table (table_priv_map) that associates privilege names with their corresponding ACL bit values. It supports both basic privileges and "WITH GRANT OPTION" variants, which allow users to grant the privilege to others. The function handles legacy "RULE" privileges by mapping them to zero (effectively ignoring them) for backward compatibility.

The actual conversion work is delegated to convert_any_priv_string, which provides a generic privilege string parsing mechanism that can handle comma-separated privilege lists and convert them using the provided mapping table.

## Parameters / Member Variables
-  (text*): A PostgreSQL text value containing the privilege specification string, which can include individual privileges like "SELECT" or composite specifications like "SELECT, INSERT"

## Dependencies
- Functions called/Symbols referenced:
  - [convert_any_priv_string](convert_any_priv_string.md): Generic privilege string parser that handles the actual conversion
  - priv_map: Structure type for privilege name-to-value mappings
  - ACL_SELECT, ACL_INSERT, ACL_UPDATE, ACL_DELETE: Basic table privilege constants
  - ACL_TRUNCATE, ACL_REFERENCES, ACL_TRIGGER, ACL_MAINTAIN: Additional table privilege constants
  - ACL_GRANT_OPTION_FOR: Macro to generate grant option variants of privileges
- Called from (representative examples):
  - [has_table_privilege_name_name](../h/has_table_privilege_name_name.md): Table privilege check with role name and table name
  - [has_table_privilege_name](../h/has_table_privilege_name.md): Table privilege check with current user and table name
  - [has_table_privilege_id_id](../h/has_table_privilege_id_id.md): Table privilege check with role ID and table ID
  - has_table_privilege functions: All table privilege checking functions

## Notes and Other Information
- This is a static function, accessible only within the acl.c source file
- Supports all standard PostgreSQL table privileges: SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER, MAINTAIN
- Each privilege can be specified with or without "WITH GRANT OPTION"
- Legacy "RULE" privileges are supported but mapped to zero for backward compatibility
- The privilege mapping is static and defined at compile time for efficiency
- Part of PostgreSQL's comprehensive access control system
- Uses the generic convert_any_priv_string function for actual parsing and conversion
- Located in src/backend/utils/adt/acl.c:2064-2107

## Simplified Source

```c
static AclMode convert_table_priv_string(text *priv_type_text) {
    // Table privilege mapping - maps privilege names to ACL bits
    static const priv_map table_priv_map[] = {
        {"SELECT", ACL_SELECT},
        {"SELECT WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_SELECT)},
        {"INSERT", ACL_INSERT},
        {"INSERT WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_INSERT)},
        {"UPDATE", ACL_UPDATE},
        {"UPDATE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_UPDATE)},
        {"DELETE", ACL_DELETE},
        {"DELETE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_DELETE)},
        {"TRUNCATE", ACL_TRUNCATE},
        {"TRUNCATE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_TRUNCATE)},
        {"REFERENCES", ACL_REFERENCES},
        {"REFERENCES WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_REFERENCES)},
        {"TRIGGER", ACL_TRIGGER},
        {"TRIGGER WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_TRIGGER)},
        {"MAINTAIN", ACL_MAINTAIN},
        {"MAINTAIN WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_MAINTAIN)},
        {"RULE", 0},  // Legacy privilege, ignore
        {"RULE WITH GRANT OPTION", 0},
        {NULL, 0}
    };

    // Use generic privilege string converter with table mapping
    return convert_any_priv_string(priv_type_text, table_priv_map);
}
```