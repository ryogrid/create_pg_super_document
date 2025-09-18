# object_aclmask

## Location
src/backend/catalog/aclchk.c: 3101 - 3111

## Overview
A simple wrapper function that provides a generic interface for examining user privileges on database objects by delegating to the extended version with default parameters.

## Definition
```c
static AclMode object_aclmask(Oid classid, Oid objectid, Oid roleid, AclMode mask, AclMaskHow how)
```

## Detailed Description
The `object_aclmask` function serves as a simplified interface to the more comprehensive `object_aclmask_ext` function. It provides a generic routine for examining user privileges on various PostgreSQL database objects without requiring the caller to specify additional snapshot parameters.

This function is part of PostgreSQL's access control system and acts as an intermediary layer that maintains API compatibility while internally using the extended functionality. It simply passes all parameters through to `object_aclmask_ext` with a NULL snapshot parameter, allowing the extended function to use the current transaction's snapshot for privilege checking.

The function is designed to handle lookup failures with full error reporting treatment, as documented in the source comments, since the has_xxx_privilege() family of functions allow users to pass arbitrary OIDs.

## Parameters / Member Variables
- `classid`: The OID of the system catalog relation that contains the object (e.g., RelationRelationId for tables)
- `objectid`: The OID of the specific database object being checked
- `roleid`: The OID of the role whose permissions are being examined
- `mask`: The access permissions being requested (AclMode bitmask)
- `how`: Specifies the method for ACL checking (AclMaskHow enum value)

## Dependencies
- Functions called/Symbols referenced:
  - [object_aclmask_ext](object_aclmask_ext.md)
  - AclMaskHow enum
- Called from (representative examples):
  - InternalDefaultACL
  - [pg_aclmask](../p/pg_aclmask.md) (multiple calls for different object types)

## Notes and Other Information
- This is a static function internal to the aclchk.c module
- Acts as a convenience wrapper around the more feature-complete `object_aclmask_ext`
- Provides backward compatibility and simplified API for callers that don't need snapshot control
- The function is heavily used by `pg_aclmask` for handling various object types like databases, functions, languages, schemas, tablespaces, foreign data wrappers, foreign servers, and types
- Error handling for invalid OIDs is implemented in the extended function this delegates to