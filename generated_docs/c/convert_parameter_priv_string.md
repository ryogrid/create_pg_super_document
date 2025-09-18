# convert_parameter_priv_string

## Location
src/backend/utils/adt/acl.c: 4676 - 4704

## Overview
A static function that converts text string representations of parameter privileges into PostgreSQL's internal AclMode bitmask format.

## Definition


## Detailed Description
This function serves as a specialized converter for parameter privilege strings, defining the mapping between human-readable privilege names and their corresponding internal AclMode values. It maintains a static privilege map that defines the valid parameter privileges in PostgreSQL and their associated ACL bits.

The function supports both basic privileges (SET, ALTER SYSTEM) and their grant option variants (SET WITH GRANT OPTION, ALTER SYSTEM WITH GRANT OPTION). It delegates the actual string parsing and conversion to the generic  function, providing the parameter-specific privilege mapping.

## Parameters / Member Variables
- : A PostgreSQL text value containing the privilege name string to be converted

## Dependencies
- Functions called/Symbols referenced:
  - priv_map (structure type for privilege mapping)
  - ACL_SET (privilege constant for SET privilege)
  - ACL_ALTER_SYSTEM (privilege constant for ALTER SYSTEM privilege)
  - ACL_GRANT_OPTION_FOR (macro to create grant option variants)
  - [convert_any_priv_string](convert_any_priv_string.md) (generic privilege string converter)
- Called from (representative examples):
  - [has_parameter_privilege_name_name](../h/has_parameter_privilege_name_name.md)
  - [has_parameter_privilege_name](../h/has_parameter_privilege_name.md)
  - [has_parameter_privilege_id_name](../h/has_parameter_privilege_id_name.md)

## Notes and Other Information
- This is a static function, accessible only within the same source file (src/backend/utils/adt/acl.c)
- Defines a static privilege map containing valid parameter privilege strings:
  - "SET" -> ACL_SET
  - "SET WITH GRANT OPTION" -> ACL_GRANT_OPTION_FOR(ACL_SET)
  - "ALTER SYSTEM" -> ACL_ALTER_SYSTEM
  - "ALTER SYSTEM WITH GRANT OPTION" -> ACL_GRANT_OPTION_FOR(ACL_ALTER_SYSTEM)
- The privilege map is null-terminated for easy iteration
- Leverages the generic privilege conversion framework through convert_any_priv_string
- Part of the parameter privilege checking infrastructure in PostgreSQL's access control system