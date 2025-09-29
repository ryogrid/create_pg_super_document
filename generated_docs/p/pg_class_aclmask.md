# pg_class_aclmask

## Location
[src/backend/catalog/aclchk.c:3329-3338](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L3329-L3338)

## Overview
A wrapper function that examines a user's privileges for a specific table/relation by checking against the relation's access control list (ACL).

## Definition

```c
AclMode
pg_class_aclmask(Oid table_oid, Oid roleid,
				 AclMode mask, AclMaskHow how)
```
## Detailed Description
This function serves as a simplified interface to , providing privilege checking for table/relation objects without the need for missing object detection. It directly delegates to the extended version with a NULL  parameter, making it suitable for cases where the caller expects the relation to exist and wants an error if it doesn't.

The function is part of PostgreSQL's access control system and is used throughout the system to verify whether a specific role has the requested permissions on a given relation before allowing operations to proceed.

## Parameters / Member Variables
- : The object identifier (OID) of the table/relation to check permissions for
- : The OID of the role whose permissions are being checked
- : Bitmask specifying which permissions to check (e.g., ACL_SELECT, ACL_INSERT, ACL_UPDATE, ACL_DELETE)
- : Specifies how to combine multiple ACL entries (ACLMASK_ALL or ACLMASK_ANY)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_class_aclmask_ext](pg_class_aclmask_ext.md)
  - AclMaskHow (enum type)
- Called from (representative examples):
  - [pg_aclmask](pg_aclmask.md)
  - [ExecCheckOneRelPerms](../E/ExecCheckOneRelPerms.md)
  - [AclResult](../A/AclResult.md)

## Notes and Other Information
- This is an exported routine specifically designed for external use when checking table privileges
- The function will throw an error if the specified relation does not exist, unlike its extended counterpart
- Part of the broader ACL (Access Control List) framework in PostgreSQL
- Located in src/backend/catalog/aclchk.c, which contains the core access control checking logic

## Simplified Source

```c
AclMode
pg_class_aclmask(Oid table_oid, Oid roleid,
                 AclMode mask, AclMaskHow how)
{
    // Delegate to extended version with no missing object handling
    return pg_class_aclmask_ext(table_oid, roleid, mask, how, NULL);
}
```