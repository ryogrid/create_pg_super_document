# recordExtObjInitPriv

## Location
src/backend/catalog/aclchk.c: 4409 - 4572

## Overview
Records the initial privileges (ACLs) for a database object and its sub-objects into pg_init_privs when the object is added to an extension, preserving the original privilege state for potential restoration.

## Definition


## Detailed Description
This function is part of PostgreSQL's extension system and handles the recording of initial privileges when objects are added to extensions via ALTER EXTENSION ADD. It stores the current ACL state of objects in pg_init_privs so that privileges can be restored when the extension is dropped or when CREATE EXTENSION is run.

The function handles different object types differently:
1. **Relations (tables, views, etc.)**: Records both table-level and column-level ACLs, iterating through all non-dropped columns to capture their individual privileges
2. **Large Objects**: Uses pg_largeobject_metadata to access ACL information (though this is currently dead code as large objects cannot be extension members)
3. **Other Objects**: Uses a generic approach with get_object_attnum_acl() to find the ACL attribute for various object types

The function skips objects that don't have permissions (indexes, partitioned indexes, composite types) and gracefully handles NULL ACLs by not recording entries for them.

## Parameters / Member Variables
- : OID of the database object whose privileges should be recorded
- : OID of the system catalog class that contains the object (e.g., RelationRelationId, ProcedureRelationId)

## Dependencies
- Functions called/Symbols referenced:
  - recordExtensionInitPrivWorker (worker function that actually inserts records into pg_init_privs)
  - SearchSysCache1, SearchSysCache2 (system catalog lookups)
  - SysCacheGetAttr (extracts attributes from cached tuples)
  - get_object_attnum_acl (gets ACL attribute number for object types)
  - get_object_catcache_oid (gets cache ID for object types)
  - get_object_class_descr (gets descriptive name for object classes)
  - DatumGetAclP (converts Datum to ACL pointer)
  - Various system catalog access functions
- Called from:
  - ExecAlterExtensionContentsRecurse (during ALTER EXTENSION ADD operations)

## Notes and Other Information
- Part of PostgreSQL's extension privilege preservation system
- Records privileges in pg_init_privs for later restoration during CREATE EXTENSION
- Handles complex cases like column-level privileges for relations
- Includes dead code for large objects (cannot currently be extension members)
- Skips objects without permissions (indexes, composite types)
- Uses different access methods based on object type (syscache vs. table scan)
- Essential for maintaining consistent privilege states across extension operations
- The recorded privileges serve as a baseline for privilege restoration when extensions are recreated
- Only records non-NULL ACLs to avoid unnecessary pg_init_privs entries