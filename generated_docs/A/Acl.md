# Acl

## Location
src/include/utils/acl.h: 106 - 107

## Overview
Acl represents an Access Control List as a one-dimensional PostgreSQL array of AclItem structures, used to manage permissions and privileges for database objects.

## Definition


## Detailed Description
Acl is a specialized PostgreSQL array type that stores access control information for database objects. It is built on top of the standard ArrayType structure but has specific constraints:

- Must be one-dimensional (no multi-dimensional arrays)
- Cannot contain null values  
- Lower bound is ignored when reading and set to 1 when writing
- Elements are toastable (can be compressed/stored out-of-line) as of PostgreSQL 7.1

The Acl array contains AclItem structures, where each AclItem represents a single permission grant consisting of a grantee (who receives the permission), grantor (who grants it), and the specific privilege bits.

Key characteristics:
- Inherits all properties of PostgreSQL's varlena ArrayType structure
- Stores permission data for tables, functions, schemas, and other database objects
- Must be properly detoasted using provided macros before access
- Used extensively throughout PostgreSQL's permission checking system

## Parameters / Member Variables
As a typedef of ArrayType, Acl inherits these members:
- : Varlena header containing total object size (accessed via VARSIZE() macros)
- : Number of dimensions (always 1 for Acl)
- : Offset to actual data, or 0 if no null bitmap
- : Element type OID (always aclitem type)

## Dependencies
- Functions called/Symbols referenced:
  - ArrayType (base structure)
  - AclItem (array element type)

- Called from (representative examples):
  - acldefault
  - aclupdate  
  - aclcopy
  - aclmerge
  - object_aclcheck
  - pg_class_aclmask
  - ExecGrant_Relation

## Notes and Other Information
- **Toasting Warning**: Always use detoasting macros (DatumGetAclP, DatumGetAclPCopy) when accessing Acl arrays, as they may be compressed or stored externally
- **Array Constraints**: Unlike general PostgreSQL arrays, Acl arrays are restricted to single dimensions with no null elements
- **Storage Format**: Follows standard PostgreSQL array layout with varlena header
- **Usage Pattern**: Primarily used in catalog tables (pg_class.relacl, pg_proc.proacl, etc.) and throughout the permission checking infrastructure
- **Performance**: Large ACLs may be toasted to improve performance and reduce tuple size