# string_to_privilege

## Location
[src/backend/catalog/aclchk.c:2615-2657](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L2615-L2657)

## Overview
Converts a string representation of a privilege name into the corresponding AclMode bitmask value used internally by PostgreSQL's access control system.

## Definition

```c
static AclMode
string_to_privilege(const char *privname)
```
## Detailed Description
This function performs case-sensitive string matching to convert human-readable privilege names (such as "select", "insert", "update") into their corresponding AclMode constants. It supports all standard PostgreSQL privilege types including table privileges (SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER), database privileges (CREATE, CONNECT, TEMPORARY), function privileges (EXECUTE), schema privileges (USAGE), and system privileges (SET, ALTER SYSTEM, MAINTAIN).

The function includes special handling for the legacy "rule" privilege type, which is ignored (returns 0) for backward compatibility. If an unrecognized privilege name is provided, the function raises an ERROR with ERRCODE_SYNTAX_ERROR.

## Parameters / Member Variables
- `*privname`: A null-terminated string containing the name of the privilege to convert (case-sensitive)
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
- [String](../S/String.md) matching is case-sensitive and exact
- Function will not return on invalid input - it raises an ERROR instead

## Simplified Source

```c
static AclMode string_to_privilege(const char *privname)
{
    // Table privileges
    if (strcmp(privname, "select") == 0)   return ACL_SELECT;
    if (strcmp(privname, "insert") == 0)   return ACL_INSERT;
    if (strcmp(privname, "update") == 0)   return ACL_UPDATE;
    if (strcmp(privname, "delete") == 0)   return ACL_DELETE;
    if (strcmp(privname, "truncate") == 0) return ACL_TRUNCATE;
    if (strcmp(privname, "references") == 0) return ACL_REFERENCES;
    if (strcmp(privname, "trigger") == 0)  return ACL_TRIGGER;

    // Function privileges
    if (strcmp(privname, "execute") == 0)  return ACL_EXECUTE;

    // Schema/Type privileges
    if (strcmp(privname, "usage") == 0)    return ACL_USAGE;

    // Database privileges
    if (strcmp(privname, "create") == 0)   return ACL_CREATE;
    if (strcmp(privname, "connect") == 0)  return ACL_CONNECT;
    if (strcmp(privname, "temporary") == 0 || strcmp(privname, "temp") == 0)
        return ACL_CREATE_TEMP;

    // System privileges
    if (strcmp(privname, "set") == 0)      return ACL_SET;
    if (strcmp(privname, "alter system") == 0) return ACL_ALTER_SYSTEM;
    if (strcmp(privname, "maintain") == 0) return ACL_MAINTAIN;

    // Legacy compatibility - ignore old rule privileges
    if (strcmp(privname, "rule") == 0)     return 0;

    // Unknown privilege name
    ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
             errmsg("unrecognized privilege type \"%s\"", privname)));
    return 0;
}
```