# ExecGrant_Largeobject

## Location
src/backend/catalog/aclchk.c: 2308 - 2443

## Overview
ExecGrant_Largeobject handles GRANT and REVOKE operations specifically for large objects, which require special handling due to their unique catalog structure.

## Definition


## Detailed Description
ExecGrant_Largeobject implements privilege management for PostgreSQL large objects (LOBs). Unlike other database objects that use syscache for catalog access, large objects require direct table scanning of pg_largeobject_metadata since there's no syscache available. The function follows the same general pattern as ExecGrant_common but with large-object-specific catalog access methods.

Large objects have their own privilege set (SELECT and UPDATE privileges) defined by ACL_ALL_RIGHTS_LARGEOBJECT. The function handles ACL modification, dependency tracking, and extension privilege recording specifically for the large object subsystem.

## Parameters / Member Variables
- : Internal representation of the GRANT/REVOKE statement containing target large object OIDs, grantees, privileges, and options

## Dependencies
- Functions called/Symbols referenced:
  - table_open, table_close (with LargeObjectMetadataRelationId)
  - ScanKeyInit, systable_beginscan, systable_getnext, systable_endscan
  - heap_getattr, heap_modify_tuple
  - acldefault, aclmembers (with OBJECT_LARGEOBJECT)
  - select_best_grantor
  - restrict_and_check_grant
  - merge_acl_with_grant
  - CatalogTupleUpdate
  - updateAclDependencies (with LargeObjectRelationId)
  - recordExtensionInitPriv
  - CommandCounterIncrement
- Called from:
  - ExecGrantStmt_oids (when processing large object privileges)

## Notes and Other Information
- Cannot use ExecGrant_common because pg_largeobject_metadata lacks syscache support
- Uses systable_beginscan with LargeObjectMetadataOidIndexId for efficient lookup
- Handles ACL_ALL_RIGHTS_LARGEOBJECT as the default privilege set for ALL PRIVILEGES
- Creates readable names like "large object 12345" for error messages
- Updates dependencies using LargeObjectRelationId rather than LargeObjectMetadataRelationId
- Large objects support SELECT (read) and UPDATE (write) privileges