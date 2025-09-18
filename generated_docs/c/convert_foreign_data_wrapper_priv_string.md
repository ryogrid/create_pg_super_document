# convert_foreign_data_wrapper_priv_string

## Location
src/backend/utils/adt/acl.c: 3368 - 3395

## Overview
Converts a text string representing foreign data wrapper privilege types into an AclMode bitmask value.

## Definition
static AclMode convert_foreign_data_wrapper_priv_string(text *priv_type_text)

## Detailed Description
This function parses privilege type strings specific to foreign data wrappers and converts them to the corresponding AclMode bitmask values. It uses a predefined privilege mapping table that defines the valid privilege types for foreign data wrappers and their corresponding ACL values.

The function supports two privilege types for foreign data wrappers:
- "USAGE": Basic usage privilege (ACL_USAGE)
- "USAGE WITH GRANT OPTION": Usage privilege with the ability to grant it to others

The function leverages the generic convert_any_priv_string utility function to perform the actual string parsing and conversion, providing it with the foreign data wrapper-specific privilege mapping table.

## Parameters / Member Variables
- : A text pointer containing the privilege type string to be converted (e.g., "USAGE", "USAGE WITH GRANT OPTION")

## Dependencies
- Functions called/Symbols referenced:
  - priv_map: Type definition for privilege mapping structure
  - ACL_USAGE: Basic usage privilege constant
  - ACL_GRANT_OPTION_FOR: Macro to create grant option for a privilege
  - convert_any_priv_string: Generic privilege string conversion function
- Called from (representative examples):
  - has_foreign_data_wrapper_privilege_name_name: Role/FDW name-based privilege checking
  - has_foreign_data_wrapper_privilege_name: Role Oid/FDW name-based privilege checking
  - has_foreign_data_wrapper_privilege_name_id: Role/FDW Oid-based privilege checking
  - has_foreign_data_wrapper_privilege_id: Role Oid/FDW Oid-based privilege checking
  - has_foreign_data_wrapper_privilege_id_name: Role Oid/FDW name-based privilege checking
  - has_foreign_data_wrapper_privilege_id_id: Role Oid/FDW Oid-based privilege checking

## Notes and Other Information
- This is a static function, accessible only within the acl.c compilation unit
- Part of the foreign data wrapper privilege checking infrastructure
- The privilege mapping table is defined as a static constant array within the function
- Foreign data wrappers only support USAGE privilege, unlike other database objects that may support multiple privilege types
- The function uses the established pattern of delegating to convert_any_priv_string for consistent privilege string parsing across different object types
- Error handling for invalid privilege strings is handled by the underlying convert_any_priv_string function