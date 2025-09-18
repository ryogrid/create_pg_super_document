# privilege_to_string

## Location
[src/backend/catalog/aclchk.c:2658-2704](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L2658-L2704)

## Overview
Converts an AclMode bitmask value into its corresponding human-readable string representation for PostgreSQL privileges.

## Definition
```c
static const char *privilege_to_string(AclMode privilege)
```

## Detailed Description
This function performs the inverse operation of string_to_privilege, converting internal AclMode constants back into their uppercase string representations. It uses a switch statement to handle all standard PostgreSQL privilege types including table privileges (SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER), database privileges (CREATE, CONNECT, TEMP), function privileges (EXECUTE), schema privileges (USAGE), and system privileges (SET, ALTER SYSTEM, MAINTAIN).

The function returns uppercase string literals for display purposes. Note that ACL_CREATE_TEMP is converted to "TEMP" rather than "TEMPORARY". If an unrecognized privilege value is provided, the function raises an ERROR via elog.

## Parameters / Member Variables
- `privilege`: An AclMode value representing a single privilege type to convert to string

## Dependencies
- Functions called/Symbols referenced:
  - ACL_INSERT
  - ACL_SELECT
  - ACL_UPDATE
  - ACL_DELETE
  - ACL_TRUNCATE
  - ACL_REFERENCES
  - ACL_TRIGGER
  - ACL_EXECUTE
  - ACL_USAGE
  - ACL_CREATE
  - ACL_CREATE_TEMP
  - ACL_CONNECT
  - ACL_SET
  - ACL_ALTER_SYSTEM
  - ACL_MAINTAIN
  - elog (for error handling)
- Called from:
  - InternalDefaultACL (src/backend/catalog/aclchk.c:137)
  - [ExecuteGrantStmt](../E/ExecuteGrantStmt.md) (src/backend/catalog/aclchk.c:587)
  - [ExecAlterDefaultPrivilegesStmt](../E/ExecAlterDefaultPrivilegesStmt.md) (src/backend/catalog/aclchk.c:1121)
  - [ExecGrant_Relation](../E/ExecGrant_Relation.md) (src/backend/catalog/aclchk.c:2098)

## Notes and Other Information
- This is a static function, only accessible within the aclchk.c compilation unit
- Returns uppercase string literals suitable for user display and error messages
- ACL_CREATE_TEMP maps to "TEMP" rather than "TEMPORARY" for conciseness
- Function will not return on invalid input - it raises an ERROR instead
- Complementary function to string_to_privilege but note the case difference (lowercase input vs uppercase output)