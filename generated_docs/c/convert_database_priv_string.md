# convert_database_priv_string

## Location
src/backend/utils/adt/acl.c: 3162 - 3195

## Overview
Converts a text string representation of database privileges to the corresponding internal AclMode bitmask.

## Definition
```c
static AclMode convert_database_priv_string(text *priv_type_text)
```

## Detailed Description
This static helper function parses database privilege names and converts them to PostgreSQL internal privilege bitmasks (AclMode). It uses a static privilege mapping table that defines the valid database privilege names and their corresponding ACL constants. The function supports standard database privileges including CREATE, TEMPORARY/TEMP, and CONNECT, as well as their "WITH GRANT OPTION" variants. The actual parsing and conversion is delegated to the generic convert_any_priv_string function.

## Parameters / Member Variables
- `priv_type_text`: PostgreSQL text object containing the privilege string to parse (e.g., "CREATE", "CONNECT", "TEMPORARY WITH GRANT OPTION")

## Dependencies
- Functions called/Symbols referenced:
  - convert_any_priv_string: Generic privilege string parser that uses the provided mapping table
  - priv_map: Structure type for privilege name to bitmask mapping
  - ACL_CREATE: Privilege constant for CREATE privilege
  - ACL_CREATE_TEMP: Privilege constant for TEMPORARY privilege  
  - ACL_CONNECT: Privilege constant for CONNECT privilege
  - ACL_GRANT_OPTION_FOR: Macro to add grant option flag to privileges
- Called from (representative examples):
  - has_database_privilege_name_name: User name + database name variant
  - has_database_privilege_name: Current user + database name variant
  - has_database_privilege_name_id: User name + database OID variant
  - has_database_privilege_id: Current user + database OID variant
  - has_database_privilege_id_name: User OID + database name variant
  - has_database_privilege_id_id: User OID + database OID variant

## Notes and Other Information
- Supports three main database privileges: CREATE, TEMPORARY (alias TEMP), and CONNECT
- Each privilege can be specified with or without "WITH GRANT OPTION"
- The privilege mapping table is null-terminated for easy iteration
- This is a static function, only accessible within the same source file
- Located in src/backend/utils/adt/acl.c:3162-3195