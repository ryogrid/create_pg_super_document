# convert_language_priv_string

## Location
[src/backend/utils/adt/acl.c:3777-3804](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L3777-L3804)

## Overview
Converts a text string representation of language privileges into an AclMode bitmask for use in PostgreSQL's access control system.

## Definition

```c
static AclMode
convert_language_priv_string(text *priv_type_text)
```
## Detailed Description
This function is a specialized privilege string converter for procedural language privileges. It maintains a static mapping table that defines the valid privilege strings for languages and their corresponding AclMode values. For procedural languages, PostgreSQL only supports the "USAGE" privilege, which can be granted with or without the grant option. The function delegates the actual parsing work to the generic  function, providing language-specific privilege mappings.

## Parameters / Member Variables
-  (text*): A PostgreSQL text value containing the privilege string to convert (e.g., "USAGE", "USAGE WITH GRANT OPTION")

## Dependencies
- Functions called/Symbols referenced:
  - [convert_any_priv_string](convert_any_priv_string.md): Generic function that parses privilege strings using a provided mapping table
  - priv_map: Structure type for privilege mapping entries
  - ACL_USAGE: Constant representing the usage privilege bit
  - ACL_GRANT_OPTION_FOR: Macro that adds the grant option bit to a privilege
- Called from (representative examples):
  - [has_language_privilege_name_name](../h/has_language_privilege_name_name.md): Checks language privileges using role name and language name
  - [has_language_privilege_name](../h/has_language_privilege_name.md): Checks language privileges for current user using language name
  - [has_language_privilege_id_id](../h/has_language_privilege_id_id.md): Checks language privileges using role OID and language OID
  - [has_language_privilege_id](../h/has_language_privilege_id.md): Checks language privileges for current user using language OID

## Notes and Other Information
- This is a static helper function, not exposed outside of acl.c
- Only supports "USAGE" and "USAGE WITH GRANT OPTION" privileges for procedural languages
- The privilege mapping table is defined locally within the function as a static constant
- Part of PostgreSQL's privilege checking infrastructure, following the same pattern as other object types
- Located in src/backend/utils/adt/acl.c:3777-3804

## Simplified Source

```c
static AclMode convert_language_priv_string(text *priv_type_text) {
    // Language privilege mapping table
    static const priv_map language_priv_map[] = {
        {"USAGE", ACL_USAGE},
        {"USAGE WITH GRANT OPTION", ACL_GRANT_OPTION_FOR(ACL_USAGE)},
        {NULL, 0}
    };

    // Use generic privilege conversion with language-specific mappings
    return convert_any_priv_string(priv_type_text, language_priv_map);
}
```