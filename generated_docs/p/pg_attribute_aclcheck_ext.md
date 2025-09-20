# pg_attribute_aclcheck_ext

## Location
[src/backend/catalog/aclchk.c:3937-3966](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L3937-L3966)

## Overview
This function checks a user's access privileges to a specific column of a table, with additional support for detecting missing attributes.

## Definition

```c
AclResult
pg_attribute_aclcheck_ext(Oid table_oid, AttrNumber attnum,
						  Oid roleid, AclMode mode, bool *is_missing)
```
## Detailed Description
The  function is an exported routine that verifies whether a specified user (role) has the requested access privileges to a particular column of a table. This is an extended version of the basic attribute ACL check that provides additional information about whether the attribute exists. The function internally uses  to perform the actual privilege checking and returns a simple ACLCHECK_OK or ACLCHECK_NO_PRIV result.

## Parameters / Member Variables
- : The OID of the table containing the column to be checked
- : The attribute number (column number) within the table
- : The OID of the role (user) whose privileges are being checked
- : The access mode being requested (e.g., ACL_SELECT, ACL_UPDATE)
- : Output parameter that indicates whether the attribute was found to be missing

## Dependencies
- Functions called/Symbols referenced:
  - [pg_attribute_aclmask_ext](pg_attribute_aclmask_ext.md)
  - ACLMASK_ANY
  - ACLCHECK_NO_PRIV
  - AclResult
- Called from (representative examples):
  - [pg_attribute_aclcheck](pg_attribute_aclcheck.md)
  - [column_privilege_check](../c/column_privilege_check.md)

## Notes and Other Information
- This function is part of PostgreSQL's access control system for column-level privileges
- The extended version provides better error reporting by distinguishing between permission denied and non-existent attributes
- Returns ACLCHECK_OK if any of the requested privileges are granted, ACLCHECK_NO_PRIV otherwise
- Located in src/backend/catalog/aclchk.c lines 3937-3966