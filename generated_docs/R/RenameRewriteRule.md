# RenameRewriteRule

## Location
[src/backend/rewrite/rewriteDefine.c:793-872](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteDefine.c#L793-L872)

## Overview
Renames an existing rewrite rule by updating the rule name in the system catalog while performing comprehensive validation and maintaining system consistency.

## Definition

```c
ObjectAddress
RenameRewriteRule(RangeVar *relation, const char *oldName,
				  const char *newName)
```
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
  - [table_open](../t/table_open.md)/table_close
  - SearchSysCacheCopy2
  - HeapTupleIsValid
  - ereport/errcode/errmsg
  - RelationGetRelationName
  - GETSTRUCT
  - [IsDefinedRewriteRule](../I/IsDefinedRewriteRule.md)
  - [namestrcpy](../n/namestrcpy.md)
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

## Simplified Source

```c
ObjectAddress
RenameRewriteRule(RangeVar *relation, const char *oldName, const char *newName)
{
    Oid relid;
    Relation targetrel;
    Relation pg_rewrite_desc;
    HeapTuple ruletup;
    Form_pg_rewrite ruleform;
    Oid ruleOid;
    ObjectAddress address;

    // Get relation OID with exclusive lock and permissions check
    relid = RangeVarGetRelidExtended(relation, AccessExclusiveLock, 0,
                                     RangeVarCallbackForRenameRule, NULL);

    // Open the target relation
    targetrel = relation_open(relid, NoLock);

    // Open pg_rewrite catalog for modification
    pg_rewrite_desc = table_open(RewriteRelationId, RowExclusiveLock);

    // Find the existing rule
    ruletup = SearchSysCacheCopy2(RULERELNAME,
                                  ObjectIdGetDatum(relid),
                                  PointerGetDatum(oldName));
    if (!HeapTupleIsValid(ruletup))
        ereport(ERROR, "rule does not exist");

    ruleform = (Form_pg_rewrite) GETSTRUCT(ruletup);
    ruleOid = ruleform->oid;

    // Check if new name already exists
    if (IsDefinedRewriteRule(relid, newName))
        ereport(ERROR, "rule with new name already exists");

    // Disallow renaming ON SELECT rules
    if (ruleform->ev_type == CMD_SELECT + '0')
        ereport(ERROR, "renaming ON SELECT rule not allowed");

    // Update the rule name
    namestrcpy(&(ruleform->rulename), newName);
    CatalogTupleUpdate(pg_rewrite_desc, &ruletup->t_self, ruletup);

    // Cleanup and cache invalidation
    InvokeObjectPostAlterHook(RewriteRelationId, ruleOid, 0);
    heap_freetuple(ruletup);
    table_close(pg_rewrite_desc, RowExclusiveLock);
    CacheInvalidateRelcache(targetrel);

    // Return object address for dependency tracking
    ObjectAddressSet(address, RewriteRelationId, ruleOid);
    relation_close(targetrel, NoLock);

    return address;
}
```