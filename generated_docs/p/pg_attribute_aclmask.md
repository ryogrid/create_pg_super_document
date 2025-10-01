# pg_attribute_aclmask

## Location
[src/backend/catalog/aclchk.c:3204-3214](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L3204-L3214)

## Overview
A wrapper function that examines user privileges specifically granted on table columns, delegating to the extended version with default parameters.

## Definition
```c
static AclMode pg_attribute_aclmask(Oid table_oid, AttrNumber attnum, Oid roleid, AclMode mask, AclMaskHow how)
```

## Detailed Description
The `pg_attribute_aclmask` function provides a simplified interface for checking column-level privileges in PostgreSQL. It serves as a wrapper around `pg_attribute_aclmask_ext`, providing backward compatibility and a cleaner API for callers that don't need extended functionality.

Importantly, this function only considers privileges granted specifically on the individual column, not relation-level privileges. It is the caller's responsibility to combine column-level privileges with appropriate table-level privileges when making access control decisions. This design allows for fine-grained permission control where column access can be granted independently of table access.

Unlike many other ACL functions, this function does not include special handling for superusers, as the comments explicitly state. This design choice ensures that column-level privilege checking is always performed explicitly, even for superusers, allowing the calling code to make appropriate decisions about combining table and column privileges.

## Parameters / Member Variables
- `table_oid`: The OID of the table containing the column
- `attnum`: The attribute number of the specific column being checked
- `roleid`: The OID of the role whose column privileges are being examined
- `mask`: The access permissions being requested (AclMode bitmask)
- `how`: Specifies the method for ACL evaluation (AclMaskHow enum)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_attribute_aclmask_ext](pg_attribute_aclmask_ext.md)
  - AclMaskHow enum
- Called from (representative examples):
  - InternalDefaultACL
  - [pg_aclmask](pg_aclmask.md)

## Notes and Other Information
- This is a static function internal to the aclchk.c module
- Only considers column-specific privileges, not table-level privileges
- No special superuser handling - caller must manage superuser privileges
- Acts as a simplified wrapper around the more comprehensive `pg_attribute_aclmask_ext`
- Used by `pg_aclmask` when handling OBJECT_COLUMN access checks
- The function design emphasizes the separation of column-level and table-level privilege checking, requiring explicit combination by callers

## Simplified Source

```c
static AclMode
pg_attribute_aclmask(Oid table_oid, AttrNumber attnum, Oid roleid,
                     AclMode mask, AclMaskHow how)
{
    // Delegate to extended version with default snapshot
    return pg_attribute_aclmask_ext(table_oid, attnum, roleid,
                                   mask, how, NULL);
}
```