# EnableDisableRule

## Location
[src/backend/rewrite/rewriteDefine.c:691-755](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteDefine.c#L691-L755)

## Overview
Changes the firing semantics of an existing rewrite rule by modifying its enabled/disabled state in the system catalog.

## Definition

```c
void
EnableDisableRule(Relation rel, const char *rulename,
				  char fires_when)
```
## Detailed Description
This function modifies the firing behavior of a PostgreSQL rewrite rule by updating the ev_enabled field in the pg_rewrite system catalog. It performs comprehensive validation including rule existence checks, permission verification, and proper catalog updates. The function handles the complete workflow: locating the rule in the system catalog, verifying user permissions, updating the rule's firing state if different from the current state, and invalidating relevant caches to ensure the change takes effect across all database backends. The operation is performed with proper locking to ensure consistency.

## Parameters / Member Variables
- `rel`: The relation (table/view) that owns the rule
- `*rulename`: Name of the rule to enable or disable
- `fires_when`: New firing state character (enabled/disabled/replica states)
## Dependencies
- Functions called/Symbols referenced:
  - RelationGetRelid
  - [table_open](../t/table_open.md)
  - SearchSysCacheCopy2
  - HeapTupleIsValid
  - ereport/errcode/errmsg
  - [get_rel_name](../g/get_rel_name.md)
  - GETSTRUCT
  - [object_ownercheck](../o/object_ownercheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [get_relkind_objtype](../g/get_relkind_objtype.md)
  - [get_rel_relkind](../g/get_rel_relkind.md)
  - [GetUserId](../G/GetUserId.md)
  - [DatumGetChar](../D/DatumGetChar.md)/CharGetDatum
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - [heap_freetuple](../h/heap_freetuple.md)
  - [table_close](../t/table_close.md)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
- Called from (representative examples):
  - [ATExecEnableDisableRule](../A/ATExecEnableDisableRule.md)

## Notes and Other Information
- Requires RowExclusiveLock on the pg_rewrite catalog relation
- Validates that the user is the owner of the target relation
- Only updates the catalog if the new state differs from the current state
- Triggers post-alter hooks for proper event handling
- Broadcasts cache invalidation messages to ensure all backends see the change
- Part of PostgreSQL's ALTER TABLE ENABLE/DISABLE RULE functionality
- Uses system cache for efficient rule lookup with RULERELNAME cache

## Simplified Source

```c
void EnableDisableRule(Relation rel, const char *rulename, char fires_when) {
    // Open pg_rewrite catalog for exclusive access
    Relation pg_rewrite_desc = table_open(RewriteRelationId, RowExclusiveLock);
    Oid owningRel = RelationGetRelid(rel);

    // Find the rule to modify
    HeapTuple ruletup = SearchSysCacheCopy2(RULERELNAME,
                                          ObjectIdGetDatum(owningRel),
                                          PointerGetDatum(rulename));
    if (!HeapTupleIsValid(ruletup))
        ereport(ERROR, "rule does not exist");

    Form_pg_rewrite ruleform = (Form_pg_rewrite) GETSTRUCT(ruletup);

    // Verify user has permission to modify this rule
    if (!object_ownercheck(RelationRelationId, ruleform->ev_class, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, "relation");

    // Update rule's firing state if it's different
    bool changed = false;
    if (DatumGetChar(ruleform->ev_enabled) != fires_when) {
        ruleform->ev_enabled = CharGetDatum(fires_when);
        CatalogTupleUpdate(pg_rewrite_desc, &ruletup->t_self, ruletup);
        changed = true;
    }

    // Cleanup and notify other backends of the change
    InvokeObjectPostAlterHook(RewriteRelationId, ruleform->oid, 0);
    heap_freetuple(ruletup);
    table_close(pg_rewrite_desc, RowExclusiveLock);

    if (changed)
        CacheInvalidateRelcache(rel);
}
```