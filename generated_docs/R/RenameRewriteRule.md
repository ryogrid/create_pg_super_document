# RenameRewriteRule

## Location
src/backend/rewrite/rewriteDefine.c: 793 - 872

## Overview
Renames an existing rewrite rule by updating the rule name in the system catalog while performing comprehensive validation and maintaining system consistency.

## Definition


## Detailed Description
This function implements PostgreSQL's ALTER RULE RENAME functionality by updating the rule name in the pg_rewrite system catalog. It performs extensive validation including relation existence, permission checks, rule existence verification, name conflict detection, and special restrictions on ON SELECT rules. The operation maintains an AccessExclusiveLock on the target relation throughout the transaction to ensure consistency. The function follows PostgreSQL's standard pattern for DDL operations: validation, catalog updates, hook invocation, and cache invalidation to ensure all backends see the changes.

## Parameters / Member Variables
- : RangeVar specifying the relation that owns the rule
- : Current name of the rule to be renamed
- : Desired new name for the rule

## Dependencies
- Functions called/Symbols referenced:
  - [RangeVarGetRelidExtended](RangeVarGetRelidExtended.md)
  - [RangeVarCallbackForRenameRule](RangeVarCallbackForRenameRule.md)
  - [relation_open](../r/relation_open.md)/relation_close
  - table_open/table_close
  - SearchSysCacheCopy2
  - HeapTupleIsValid
  - ereport/errcode/errmsg
  - RelationGetRelationName
  - GETSTRUCT
  - [IsDefinedRewriteRule](../I/IsDefinedRewriteRule.md)
  - namestrcpy
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - [heap_freetuple](../h/heap_freetuple.md)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
  - ObjectAddressSet
- Called from (representative examples):
  - [ExecRenameStmt](../E/ExecRenameStmt.md)

## Notes and Other Information
- Maintains AccessExclusiveLock on the relation throughout the transaction
- Prohibits renaming ON SELECT rules (which must be named "_RETURN")
- Uses RangeVarCallbackForRenameRule for pre-lock validation
- Checks for name conflicts with existing rules on the same relation
- Returns an ObjectAddress for the renamed rule for dependency tracking
- Invalidates relation cache to ensure all backends see the rule name change
- Part of PostgreSQL's ALTER RULE RENAME TO command implementation
- Uses RULERELNAME system cache for efficient rule lookup by relation and name