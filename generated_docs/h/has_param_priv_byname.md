# has_param_priv_byname

## Location
src/backend/utils/adt/acl.c: 4615 - 4627

## Overview
A static helper function that checks if a specific role has the given privileges on a parameter identified by its text name.

## Definition


## Detailed Description
This internal helper function performs parameter privilege checking by converting the parameter name from PostgreSQL's text type to a C string and then delegating the actual privilege check to . It serves as a bridge between the higher-level parameter privilege functions that work with PostgreSQL data types and the lower-level ACL checking mechanism that works with C strings.

The function is part of PostgreSQL's access control system for configuration parameters, allowing fine-grained control over which users can view or modify specific server parameters.

## Parameters / Member Variables
- : The OID of the role (user) whose privileges are being checked
- : The name of the parameter as a PostgreSQL text type
- : The privilege mode being checked (of type AclMode)

## Dependencies
- Functions called/Symbols referenced:
  - text_to_cstring (converts PostgreSQL text to C string)
  - [pg_parameter_aclcheck](../p/pg_parameter_aclcheck.md) (performs the actual ACL check)
- Called from (representative examples):
  - [has_parameter_privilege_name_name](has_parameter_privilege_name_name.md)
  - [has_parameter_privilege_name](has_parameter_privilege_name.md)
  - [has_parameter_privilege_id_name](has_parameter_privilege_id_name.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same source file (src/backend/utils/adt/acl.c)
- Returns true if the user has been granted the specified privilege, false otherwise
- The function handles the data type conversion needed to interface with the underlying ACL checking system
- Part of the family of has_parameter_privilege functions that provide different interfaces for parameter privilege checking