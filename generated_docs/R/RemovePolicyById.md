# RemovePolicyById

## Location
[src/backend/commands/policy.c:332-415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/policy.c#L332-L415)

## Overview
Removes a row-level security policy from the system by its OID, performing validation checks and cache invalidation to maintain system consistency.

## Definition
```c
void RemovePolicyById(Oid policy_id)
```

## Detailed Description
This function handles the complete removal of a row-level security policy from the PostgreSQL system. It performs several critical operations in sequence:

1. **Policy Lookup**: Scans the pg_policy catalog using the PolicyOidIndexId index to locate the policy by its OID
2. **Validation**: Verifies the policy exists and raises an error if not found
3. **Relation Locking**: Opens the target relation with AccessExclusiveLock to prevent concurrent access during policy removal
4. **Permission Checks**: Validates that:
   - The target is actually a table (regular or partitioned)
   - System table modifications are allowed if applicable
5. **Catalog Deletion**: Removes the policy tuple from pg_policy using CatalogTupleDelete
6. **Cache Invalidation**: Invalidates the relation cache to ensure all sessions see the policy change
7. **Cleanup**: Properly closes all opened relations and releases locks

The function is designed to maintain transactional consistency and handles both regular tables and partitioned tables. It includes important safety checks to prevent unauthorized system catalog modifications.

## Parameters / Member Variables
- `policy_id`: The OID of the policy to be removed from the system

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md) (relation opening)
  - [table_close](../t/table_close.md) (relation closing)
  - [ScanKeyInit](../S/ScanKeyInit.md) (scan key initialization)
  - [systable_beginscan](../s/systable_beginscan.md) (system catalog scan)
  - [systable_getnext](../s/systable_getnext.md) (scan result retrieval)
  - [systable_endscan](../s/systable_endscan.md) (scan cleanup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (tuple data extraction)
  - elog (error logging)
  - ereport (error reporting)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md) (catalog tuple deletion)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md) (cache invalidation)
  - RelationGetRelationName (relation name retrieval)
  - [IsSystemRelation](../I/IsSystemRelation.md) (system relation detection)
  - PolicyRelationId (pg_policy catalog OID)
  - PolicyOidIndexId (policy OID index)
  - AccessExclusiveLock (exclusive locking constant)
  - RowExclusiveLock (row exclusive locking constant)
  - NoLock (no lock constant)

- Called from:
  - [doDeletion](../d/doDeletion.md) (dependency system for cascaded deletions)
  - Policy management operations

## Notes and Other Information
- This is a public function, accessible from other PostgreSQL modules
- Requires RowExclusiveLock on pg_policy and AccessExclusiveLock on the target relation
- The AccessExclusiveLock on the target relation is held until transaction commit to prevent queries from seeing inconsistent policy states
- Supports both regular tables (RELKIND_RELATION) and partitioned tables (RELKIND_PARTITIONED_TABLE)
- System table modification protection is enforced through allowSystemTableMods global variable
- Cache invalidation ensures that all database sessions immediately see the policy removal
- The function maintains the important distinction that relrowsecurity flag behavior is independent of policy existence - [when](../w/when.md) set by users, it enforces default-deny behavior even without explicit policies
- Error handling includes specific error codes: ERRCODE_WRONG_OBJECT_TYPE for non-table relations, ERRCODE_INSUFFICIENT_PRIVILEGE for system catalog restrictions
- The function is typically called through PostgreSQL's dependency management system during DROP POLICY operations or cascaded deletions

## Simplified Source

```c
void RemovePolicyById(Oid policy_id)
{
    Relation pg_policy_rel;
    SysScanDesc sscan;
    ScanKeyData skey[1];
    HeapTuple tuple;
    Oid relid;
    Relation rel;

    // Open pg_policy catalog table
    pg_policy_rel = table_open(PolicyRelationId, RowExclusiveLock);

    // Scan for the policy by OID
    ScanKeyInit(&skey[0], Anum_pg_policy_oid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(policy_id));

    sscan = systable_beginscan(pg_policy_rel, PolicyOidIndexId, true,
                               NULL, 1, skey);

    tuple = systable_getnext(sscan);

    // Verify policy exists
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "could not find tuple for policy %u", policy_id);

    // Get the relation this policy belongs to and lock it exclusively
    relid = ((Form_pg_policy) GETSTRUCT(tuple))->polrelid;
    rel = table_open(relid, AccessExclusiveLock);

    // Validate target is a table
    if (rel->rd_rel->relkind != RELKIND_RELATION &&
        rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                       errmsg("\"%s\" is not a table", RelationGetRelationName(rel))));

    // Check system table modification permissions
    if (!allowSystemTableMods && IsSystemRelation(rel))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                       errmsg("permission denied: \"%s\" is a system catalog",
                             RelationGetRelationName(rel))));

    // Delete the policy tuple
    CatalogTupleDelete(pg_policy_rel, &tuple->t_self);

    systable_endscan(sscan);

    // Invalidate relation cache to propagate policy changes
    CacheInvalidateRelcache(rel);

    // Cleanup
    table_close(rel, NoLock);
    table_close(pg_policy_rel, RowExclusiveLock);
}
```