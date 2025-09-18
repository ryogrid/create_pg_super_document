# acldefault

## Location
src/backend/utils/adt/acl.c: 803 - 919

## Overview
Creates an Access Control List (ACL) describing the default access permissions for newly-created database objects based on their type and owner.

## Definition
```c
Acl *acldefault(ObjectType objtype, Oid ownerId)
```

## Detailed Description
The acldefault function is the central mechanism in PostgreSQL for establishing default access permissions for database objects. It generates an ACL structure that contains the hard-wired default privileges that apply when no explicit permissions have been granted and no pg_default_acl entries exist for the object type.

The function operates by switching on the object type and setting appropriate world (public) and owner default privileges. For most object types, the public has no rights by default, while the owner receives all applicable rights for that object type. However, some object types like databases, functions, languages, and types grant certain privileges to the public by default for backward compatibility and usability reasons.

The resulting ACL contains at most two entries: one for public access (if any) and one for the owner. The owner's entry shows all ordinary privileges but no grant options, as grant options are considered to come "from the system" rather than being self-granted, though ordinary privileges are treated as self-granted to allow the owner to revoke them.

## Parameters / Member Variables
- `objtype`: The type of database object (ObjectType enum) for which to create default permissions
- `ownerId`: The OID of the object's owner, used to set owner privileges in the ACL

## Dependencies
- Functions called/Symbols referenced:
  - allocacl (allocates ACL structure)
  - ACL_DAT (macro to access ACL data)
  - ACLITEM_SET_PRIVS_GOPTIONS (macro to set privileges and grant options)
  - Various ACL constants (ACL_NO_RIGHTS, ACL_ALL_RIGHTS_*, ACL_EXECUTE, etc.)
  - ObjectType enum values (OBJECT_TABLE, OBJECT_FUNCTION, etc.)
- Called from (representative examples):
  - SetDefaultACL (setting default ACLs)
  - ExecGrant_* functions (grant command execution)
  - object_aclmask_ext (permission checking)
  - pg_dump utilities (dumping ACL information)

## Notes and Other Information
- This function encodes PostgreSQL's hard-wired default permission policy
- Changes to this function require updating the GRANT documentation
- Default privileges by object type:
  - COLUMN: No default privileges for anyone
  - TABLE/SEQUENCE/LARGEOBJECT/SCHEMA/TABLESPACE/FDW/FOREIGN_SERVER/PARAMETER_ACL: Owner gets all rights, public gets none
  - DATABASE: Public gets CREATE TEMP and CONNECT, owner gets all rights
  - FUNCTION: Public gets EXECUTE, owner gets all rights
  - LANGUAGE: Public gets USAGE, owner gets all rights  
  - DOMAIN/TYPE: Public gets USAGE, owner gets all rights
- The ACL_ID_PUBLIC constant represents privileges granted to all users
- Owner privileges are marked as self-granted but grant options come from the system
- Used extensively throughout PostgreSQL's permission checking and pg_dump functionality