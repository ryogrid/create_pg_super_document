# AlterPolicy

## Location
[src/backend/commands/policy.c:768-1095](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/policy.c#L768-L1095)

## Overview
Handles the execution of the ALTER POLICY command by modifying an existing row-level security policy's attributes including roles, USING clause, and WITH CHECK clause while maintaining proper dependency relationships.

## Definition

```c
struct_array_builtin(role_oids, nitems, OIDOID);
```
## Detailed Description
This function implements the ALTER POLICY SQL command through a comprehensive modification process:

1. **Policy Lookup**: Locates the existing policy by table OID and policy name, validating its existence
2. **Selective Updates**: Only modifies policy attributes that are explicitly specified in the ALTER statement (roles, USING clause, or WITH CHECK clause)
3. **Expression Processing**: For updated clauses, parses and transforms new expressions; for unchanged clauses, reconstructs dependencies from existing catalog data
4. **Command Validation**: Ensures clause combinations remain valid for the policy's command type (e.g., INSERT policies can't have USING clauses)
5. **Dependency Maintenance**: Completely recreates all dependency records to reflect the new policy state
6. **Atomic Updates**: Uses catalog tuple modification with proper locking to ensure consistency

The function handles partial updates efficiently by preserving unchanged attributes and only processing modified components.

## Parameters
- : AlterPolicyStmt structure containing modification details including:
  - Policy name and target table to identify the policy
  - Optional new roles list (NULL if unchanged)
  - Optional new USING clause expression (NULL if unchanged)
  - Optional new WITH CHECK clause expression (NULL if unchanged)

## Dependencies
- Functions called/Symbols referenced:
  - [policy_role_list_to_array](../p/policy_role_list_to_array.md), construct_array_builtin (role processing)
  - [RangeVarGetRelidExtended](../R/RangeVarGetRelidExtended.md), relation_open (table access)
  - [make_parsestate](../m/make_parsestate.md), transformWhereClause, assign_expr_collations (expression parsing)
  - [systable_beginscan](../s/systable_beginscan.md), systable_getnext (policy lookup)
  - [heap_getattr](../h/heap_getattr.md), heap_modify_tuple, heap_freetuple (tuple manipulation)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (catalog updates)
  - [deleteDependencyRecordsFor](../d/deleteDependencyRecordsFor.md), deleteSharedDependencyRecordsFor (dependency cleanup)
  - [recordDependencyOn](../r/recordDependencyOn.md), recordDependencyOnExpr, recordSharedDependencyOn (dependency creation)
  - [stringToNode](../s/stringToNode.md), nodeToString (expression serialization)
  - InvokeObjectPostAlterHook, CacheInvalidateRelcache (hooks and cache management)
- Called from:
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (main utility command dispatcher)

## Notes and Other Information
- Requires AccessExclusiveLock on target table to prevent concurrent operations
- Validates command-specific clause restrictions (same as CREATE POLICY)
- Handles NULL values for unchanged attributes by preserving existing catalog data
- Reconstructs range tables for unchanged expressions to maintain proper dependencies
- Does not create dependencies on the PUBLIC role
- Returns ObjectAddress of the modified policy for use by event and dependency systems
- Completely rebuilds all dependency records rather than incrementally updating them for simplicity and correctness

## Simplified Source

```c
ObjectAddress
AlterPolicy(AlterPolicyStmt *stmt)
{
    Relation pg_policy_rel, target_table;
    Oid policy_id, table_id;
    HeapTuple policy_tuple, new_tuple;
    ScanKeyData skey[2];
    SysScanDesc sscan;

    Datum role_oids = NULL;
    int nitems = 0;
    ArrayType *role_ids = NULL;
    Node *qual = NULL, *with_check_qual = NULL;
    List *qual_parse_rtable = NIL, *with_check_parse_rtable = NIL;

    Datum values[Natts_pg_policy];
    bool isnull[Natts_pg_policy], replaces[Natts_pg_policy];
    ObjectAddress target, myself;

    // Parse new roles if provided
    if (stmt->roles != NULL) {
        role_oids = policy_role_list_to_array(stmt->roles, &nitems);
        role_ids = construct_array_builtin(role_oids, nitems, OIDOID);
    }

    // Get table ID and lock it
    table_id = RangeVarGetRelidExtended(stmt->table, AccessExclusiveLock, 0,
                                       RangeVarCallbackForPolicy, (void *) stmt);
    target_table = relation_open(table_id, NoLock);

    // Parse new USING clause if provided
    if (stmt->qual) {
        ParseState *qual_pstate = make_parsestate(NULL);
        nsitem = addRangeTableEntryForRelation(qual_pstate, target_table,
                                              AccessShareLock, NULL, false, false);
        addNSItemToQuery(qual_pstate, nsitem, false, true, true);
        qual = transformWhereClause(qual_pstate, stmt->qual, EXPR_KIND_POLICY, "POLICY");
        assign_expr_collations(qual_pstate, qual);
        qual_parse_rtable = qual_pstate->p_rtable;
        free_parsestate(qual_pstate);
    }

    // Parse new WITH CHECK clause if provided
    if (stmt->with_check) {
        // Similar parsing process for WITH CHECK clause
        ParseState *with_check_pstate = make_parsestate(NULL);
        // ... parsing logic similar to qual ...
    }

    // Find the policy to update
    pg_policy_rel = table_open(PolicyRelationId, RowExclusiveLock);
    ScanKeyInit(&skey[0], Anum_pg_policy_polrelid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(table_id));
    ScanKeyInit(&skey[1], Anum_pg_policy_polname, BTEqualStrategyNumber,
                F_NAMEEQ, CStringGetDatum(stmt->policy_name));

    sscan = systable_beginscan(pg_policy_rel, PolicyPolrelidPolnameIndexId,
                              true, NULL, 2, skey);
    policy_tuple = systable_getnext(sscan);

    if (!HeapTupleIsValid(policy_tuple))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                       errmsg("policy \"%s\" for table \"%s\" does not exist",
                             stmt->policy_name, RelationGetRelationName(target_table))));

    // Validate command-specific constraints
    polcmd = DatumGetChar(heap_getattr(policy_tuple, Anum_pg_policy_polcmd,
                                      RelationGetDescr(pg_policy_rel), &polcmd_isnull));

    if ((polcmd == ACL_SELECT_CHR || polcmd == ACL_DELETE_CHR) && stmt->with_check != NULL)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                       errmsg("only USING expression allowed for SELECT, DELETE")));

    // Build updated tuple
    memset(values, 0, sizeof(values));
    memset(replaces, 0, sizeof(replaces));
    memset(isnull, 0, sizeof(isnull));

    if (role_ids != NULL) {
        replaces[Anum_pg_policy_polroles - 1] = true;
        values[Anum_pg_policy_polroles - 1] = PointerGetDatum(role_ids);
    }

    if (qual != NULL) {
        replaces[Anum_pg_policy_polqual - 1] = true;
        values[Anum_pg_policy_polqual - 1] = CStringGetTextDatum(nodeToString(qual));
    }

    if (with_check_qual != NULL) {
        replaces[Anum_pg_policy_polwithcheck - 1] = true;
        values[Anum_pg_policy_polwithcheck - 1] = CStringGetTextDatum(nodeToString(with_check_qual));
    }

    // Update the policy
    new_tuple = heap_modify_tuple(policy_tuple, RelationGetDescr(pg_policy_rel),
                                 values, isnull, replaces);
    CatalogTupleUpdate(pg_policy_rel, &new_tuple->t_self, new_tuple);

    // Rebuild all dependencies
    policy_id = ((Form_pg_policy) GETSTRUCT(policy_tuple))->oid;
    deleteDependencyRecordsFor(PolicyRelationId, policy_id, false);
    deleteSharedDependencyRecordsFor(PolicyRelationId, policy_id, 0);

    // Record new dependencies
    myself.classId = PolicyRelationId;
    myself.objectId = policy_id;
    myself.objectSubId = 0;

    recordDependencyOnExpr(&myself, qual, qual_parse_rtable, DEPENDENCY_NORMAL);
    recordDependencyOnExpr(&myself, with_check_qual, with_check_parse_rtable, DEPENDENCY_NORMAL);

    // Record role dependencies
    for (i = 0; i < nitems; i++) {
        target.objectId = DatumGetObjectId(role_oids[i]);
        if (target.objectId != ACL_ID_PUBLIC)
            recordSharedDependencyOn(&myself, &target, SHARED_DEPENDENCY_POLICY);
    }

    InvokeObjectPostAlterHook(PolicyRelationId, policy_id, 0);
    CacheInvalidateRelcache(target_table);

    // Cleanup
    systable_endscan(sscan);
    relation_close(target_table, NoLock);
    table_close(pg_policy_rel, RowExclusiveLock);

    return myself;
}
```