# EnableDisableRule

## Location
src/backend/rewrite/rewriteDefine.c: 691 - 755

## Overview
Changes the firing semantics of an existing rewrite rule by modifying its enabled/disabled state in the system catalog.

## Definition


## Detailed Description
This function modifies the firing behavior of a PostgreSQL rewrite rule by updating the ev_enabled field in the pg_rewrite system catalog. It performs comprehensive validation including rule existence checks, permission verification, and proper catalog updates. The function handles the complete workflow: locating the rule in the system catalog, verifying user permissions, updating the rule's firing state if different from the current state, and invalidating relevant caches to ensure the change takes effect across all database backends. The operation is performed with proper locking to ensure consistency.

## Parameters / Member Variables
- : The relation (table/view) that owns the rule
- : Name of the rule to enable or disable  
- : New firing state character (enabled/disabled/replica states)

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetRelid
  - table_open
  - SearchSysCacheCopy2
  - HeapTupleIsValid
  - ereport/errcode/errmsg
  - get_rel_name
  - GETSTRUCT
  - object_ownercheck
  - aclcheck_error
  - get_relkind_objtype
  - get_rel_relkind
  - GetUserId
  - DatumGetChar/CharGetDatum
  - CatalogTupleUpdate
  - InvokeObjectPostAlterHook
  - heap_freetuple
  - table_close
  - CacheInvalidateRelcache
- Called from (representative examples):
  - ATExecEnableDisableRule

## Notes and Other Information
- Requires RowExclusiveLock on the pg_rewrite catalog relation
- Validates that the user is the owner of the target relation
- Only updates the catalog if the new state differs from the current state
- Triggers post-alter hooks for proper event handling
- Broadcasts cache invalidation messages to ensure all backends see the change
- Part of PostgreSQL's ALTER TABLE ENABLE/DISABLE RULE functionality
- Uses system cache for efficient rule lookup with RULERELNAME cache