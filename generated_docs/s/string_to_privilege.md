# string_to_privilege

## Location
[src/backend/catalog/aclchk.c:2615-2657](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L2615-L2657)

## Overview
Converts a string representation of a privilege name into the corresponding AclMode bitmask value used internally by PostgreSQL's access control system.

## Definition


## Detailed Description
This function performs case-sensitive string matching to convert human-readable privilege names (such as "select", "insert", "update") into their corresponding AclMode constants. It supports all standard PostgreSQL privilege types including table privileges (SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER), database privileges (CREATE, CONNECT, TEMPORARY), function privileges (EXECUTE), schema privileges (USAGE), and system privileges (SET, ALTER SYSTEM, MAINTAIN).

The function includes special handling for the legacy "rule" privilege type, which is ignored (returns 0) for backward compatibility. If an unrecognized privilege name is provided, the function raises an ERROR with ERRCODE_SYNTAX_ERROR.

## Parameters / Member Variables
- : A null-terminated string containing the name of the privilege to convert (case-sensitive)

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
  - ereport (for error handling)
- Called from:
  - InternalDefaultACL (src/backend/catalog/aclchk.c:136)
  - [ExecuteGrantStmt](../E/ExecuteGrantStmt.md) (src/backend/catalog/aclchk.c:582)
  - [ExecAlterDefaultPrivilegesStmt](../E/ExecAlterDefaultPrivilegesStmt.md) (src/backend/catalog/aclchk.c:1116) 
  - [ExecGrant_Relation](../E/ExecGrant_Relation.md) (src/backend/catalog/aclchk.c:2092)

## Notes and Other Information
- This is a static function, only accessible within the aclchk.c compilation unit
- Both "temporary" and "temp" strings map to the same ACL_CREATE_TEMP privilege
- The legacy "rule" privilege type is maintained for compatibility but effectively ignored
- String matching is case-sensitive and exact
- Function will not return on invalid input - it raises an ERROR instead