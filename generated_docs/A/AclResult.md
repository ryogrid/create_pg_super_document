# AclResult

## Location
src/include/utils/acl.h: 186 - 290

## Overview
AclResult is an enumeration that represents the possible outcomes of access control (permission) checks in PostgreSQL, indicating whether a requested operation is allowed or why it was denied.

## Definition


## Detailed Description
AclResult is a fundamental enumeration used throughout PostgreSQL's access control system to communicate the results of permission checks. It provides a standardized way to report whether a user has the necessary privileges to perform a requested database operation.

The enumeration supports a hierarchical permission model where operations may require either specific privileges or ownership. This allows PostgreSQL to distinguish between different types of access denials and provide appropriate error messages to users.

All permission checking functions in PostgreSQL return AclResult values, making it the standard interface for access control decisions throughout the system.

## Parameters / Member Variables
- : Permission check succeeded - the user has the required privileges or ownership to perform the operation
- : Permission denied due to insufficient privileges - the user lacks the specific ACL permissions needed
- : Permission denied because the operation requires object ownership and the user is not the owner

## Dependencies
- Functions called/Symbols referenced:
  - Used as return type by numerous ACL checking functions

- Called from (representative examples):
  - object_aclcheck
  - pg_class_aclcheck  
  - pg_attribute_aclcheck
  - pg_parameter_aclcheck
  - aclcheck_error
  - has_table_privilege_*
  - has_database_privilege_*

## Notes and Other Information
- **Error Handling**: AclResult values are typically passed to aclcheck_error() functions to generate appropriate error messages
- **Privilege Hierarchy**: The distinction between ACLCHECK_NO_PRIV and ACLCHECK_NOT_OWNER allows PostgreSQL to enforce operations that require ownership versus those that can be granted via ACL privileges
- **Return Value Convention**: ACLCHECK_OK (value 0) follows the C convention where 0 indicates success
- **Usage Pattern**: Most permission checking follows the pattern: perform check, examine AclResult, either proceed or call error handler
- **Extension Throughout Codebase**: Used extensively in catalog operations, DDL commands, function calls, and data access operations
- **Privilege Types**: Works with all PostgreSQL privilege types (SELECT, INSERT, UPDATE, DELETE, EXECUTE, USAGE, CREATE, etc.)