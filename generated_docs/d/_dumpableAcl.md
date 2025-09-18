# _dumpableAcl

## Location
[src/bin/pg_dump/pg_dump.h:162-168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L162-L168)

## Overview
The  structure stores Access Control List (ACL) information for database objects, including current permissions, default permissions, and initial privileges for objects that support ACL-based security.

## Definition


## Detailed Description
The  structure is a specialized sub-structure that must immediately follow the  base struct for any database object type that supports Access Control Lists. This structure encapsulates all ACL-related information needed for proper dumping and restoration of object permissions in PostgreSQL databases.

The structure stores both the current ACL state and default ACLs, which is crucial for generating accurate GRANT/REVOKE statements during dump output. It also handles initial privileges from the  system catalog, which tracks the original permissions state of extension objects, allowing proper restoration of security settings during database migration or backup restoration.

## Parameters / Member Variables
- : String representation of the object's current Access Control List, containing all granted permissions
- : Default ACL string appropriate for this object type and owner, used as baseline for permission comparisons
- : Character indicating the entry type from pg_init_privs catalog ('i' for initial, 'e' for extension, or 0 if no entry exists)
- : Initial ACL string from pg_init_privs catalog entry, or NULL if no initial privileges are recorded

## Dependencies
- Functions called/Symbols referenced:
  - Used in conjunction with DumpableObject base structure
  - References pg_init_privs system catalog data
- Called from (representative examples):
  - Embedded within object-specific structures that support ACLs
  - Processed by ACL dumping functions in pg_dump

## Notes and Other Information
This structure must immediately follow the DumpableObject base struct in memory layout for any object type supporting ACLs. The design allows pg_dump to cast between the base object type and the ACL-extended type as needed. The  field helps distinguish between different sources of initial privileges, particularly important for extension objects where original and current permissions may differ. The structure supports PostgreSQL's sophisticated permission model while maintaining compatibility with the broader object dumping framework.