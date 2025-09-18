# RangeVarCallbackForRenameRule

## Location
src/backend/rewrite/rewriteDefine.c: 756 - 792

## Overview
A callback function that performs permissions and integrity checks before acquiring a relation lock during rule renaming operations.

## Definition


## Detailed Description
This static callback function is invoked during the relation lock acquisition process for rule rename operations. It validates that the target relation supports rules, ensures the user has appropriate permissions, and prevents modifications to system catalogs when not allowed. The function follows PostgreSQL's standard pattern for RangeVar callbacks, which are used to perform validation checks before acquiring locks on relations. It checks relation kind compatibility (only tables, views, and partitioned tables can have rules), system catalog protection, and ownership requirements.

## Parameters / Member Variables
- : Pointer to the RangeVar structure containing the relation name and schema information
- : Object identifier of the relation being processed
- : Previous relation OID (used for concurrent drop detection)
- : Generic argument pointer (unused in this callback)

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - ObjectIdGetDatum
  - HeapTupleIsValid
  - GETSTRUCT
  - ereport/errcode/errmsg
  - errdetail_relkind_not_supported
  - IsSystemClass
  - object_ownercheck
  - aclcheck_error
  - get_relkind_objtype
  - get_rel_relkind
  - GetUserId
  - ReleaseSysCache
- Called from (representative examples):
  - RenameRewriteRule (via RangeVarGetRelidExtended)

## Notes and Other Information
- Handles concurrent relation drops gracefully by checking tuple validity
- Only allows rules on tables (RELKIND_RELATION), views (RELKIND_VIEW), and partitioned tables (RELKIND_PARTITIONED_TABLE)
- Respects the allowSystemTableMods setting for system catalog protection
- Requires relation ownership for rule rename operations
- Part of PostgreSQL's lock acquisition safety mechanism using RangeVar callbacks
- The function is static, limiting its scope to rewriteDefine.c
- Uses the RELOID system cache for efficient relation metadata lookup