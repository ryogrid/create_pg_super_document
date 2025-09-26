# AclResult

## Location
[src/include/utils/acl.h:186-290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/acl.h#L186-L290)

## Overview
AclResult is an enumeration that represents the possible outcomes of access control (permission) checks in PostgreSQL, indicating whether a requested operation is allowed or why it was denied.

## Definition

```c
typedef enum
{
	ACLCHECK_OK = 0,
	ACLCHECK_NO_PRIV,
	ACLCHECK_NOT_OWNER,
} AclResult;
```
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
  - [object_aclcheck](../o/object_aclcheck.md)
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md)  
  - [pg_attribute_aclcheck](../p/pg_attribute_aclcheck.md)
  - [pg_parameter_aclcheck](../p/pg_parameter_aclcheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - has_table_privilege_*
  - has_database_privilege_*

## Notes and Other Information
- **Error Handling**: AclResult values are typically passed to aclcheck_error() functions to generate appropriate error messages
- **Privilege Hierarchy**: The distinction between ACLCHECK_NO_PRIV and ACLCHECK_NOT_OWNER allows PostgreSQL to enforce operations that require ownership versus those that can be granted via ACL privileges
- **Return Value Convention**: ACLCHECK_OK (value 0) follows the C convention where 0 indicates success
- **Usage Pattern**: Most permission checking follows the pattern: perform check, examine AclResult, either proceed or call error handler
- **Extension Throughout Codebase**: Used extensively in catalog operations, DDL commands, function calls, and data access operations
- **Privilege Types**: Works with all PostgreSQL privilege types (SELECT, INSERT, UPDATE, DELETE, EXECUTE, USAGE, CREATE, etc.)