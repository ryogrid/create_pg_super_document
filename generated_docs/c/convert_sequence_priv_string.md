# convert_sequence_priv_string

## Location
[src/backend/utils/adt/acl.c:2301-2333](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L2301-L2333)

## Overview
Converts a text string representation of sequence privileges to an AclMode bitmask value.

## Definition
```c
static AclMode convert_sequence_priv_string(text *priv_type_text)
```

## Detailed Description
This function is a static helper function that converts human-readable privilege strings for sequences into internal AclMode bitmask representations. It defines a mapping table (sequence_priv_map) that associates privilege names with their corresponding ACL constants. The function supports the three main sequence privileges: USAGE, SELECT, and UPDATE, each with optional "WITH GRANT OPTION" variants. The actual conversion logic is delegated to convert_any_priv_string, which handles the parsing of comma-separated privilege lists and bitmask generation using the provided mapping table.

## Parameters / Member Variables
- `priv_type_text`: Text string containing comma-separated privilege names to convert

## Dependencies
- Functions called/Symbols referenced:
  - [convert_any_priv_string](convert_any_priv_string.md): Generic privilege string parser that uses the mapping table
  - ACL_USAGE: Usage privilege constant
  - ACL_SELECT: Select privilege constant  
  - ACL_UPDATE: Update privilege constant
  - ACL_GRANT_OPTION_FOR: Macro to set grant option bits for privileges
- Called from (representative examples):
  - [has_sequence_privilege_name_name](../h/has_sequence_privilege_name_name.md): Line 2119
  - [has_sequence_privilege_name](../h/has_sequence_privilege_name.md): Line 2149
  - [has_sequence_privilege_name_id](../h/has_sequence_privilege_name_id.md): Line 2180
  - [has_sequence_privilege_id](../h/has_sequence_privilege_id.md): Line 2216
  - [has_sequence_privilege_id_name](../h/has_sequence_privilege_id_name.md): Line 2249
  - [has_sequence_privilege_id_id](../h/has_sequence_privilege_id_id.md): Line 2278

## Notes and Other Information
- Static function, only accessible within the same source file
- Defines sequence-specific privilege mapping including grant options
- Supports USAGE, SELECT, and UPDATE privileges for sequences
- Uses convert_any_priv_string for the actual parsing logic
- Part of PostgreSQL's ACL (Access Control List) system
- Defined in src/backend/utils/adt/acl.c:2301-2333