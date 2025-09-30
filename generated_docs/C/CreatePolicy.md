# CreatePolicy

## Location
[src/backend/commands/policy.c:569-767](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/policy.c#L569-L767)

## Overview
Handles the execution of the CREATE POLICY command by creating a new row-level security policy in the pg_policy system catalog with specified access control rules and role assignments.

## Definition

```c
struct_array_builtin(role_oids, nitems, OIDOID);
```
## Detailed Description
This function implements the CREATE POLICY SQL command by performing comprehensive validation and catalog operations:

1. **Command Validation**: Validates policy command type (SELECT, INSERT, UPDATE, DELETE) and ensures proper clause combinations (e.g., WITH CHECK not allowed for SELECT/DELETE)
2. **Role Processing**: Converts the list of applicable roles into an array format suitable for catalog storage
3. **Expression Parsing**: Transforms USING and WITH CHECK clauses into internal expression trees using separate parse states for each
4. **Catalog Operations**: Creates a new entry in pg_policy with generated OID and validates uniqueness of policy names per table
5. **Dependency Management**: Records dependencies on the target table, referenced expressions, and applicable roles
6. **Security Integration**: Handles both permissive and restrictive policy types with proper expression collation assignment

The function ensures atomicity through catalog locking and maintains referential integrity through the dependency system.

## Parameters
- : CreatePolicyStmt structure containing all policy definition details including:
  - Policy name and target table
  - Command type (SELECT/INSERT/UPDATE/DELETE)
  - Applicable roles list
  - USING clause expression (qualification)
  - WITH CHECK clause expression (for INSERT/UPDATE)
  - Permissive/restrictive policy type

## Dependencies
- Functions called/Symbols referenced:
  - [parse_policy_command](../p/parse_policy_command.md) (command type parsing)
  - [policy_role_list_to_array](../p/policy_role_list_to_array.md), construct_array_builtin (role array construction)
  - [make_parsestate](../m/make_parsestate.md), transformWhereClause (expression parsing)
  - [RangeVarGetRelidExtended](../R/RangeVarGetRelidExtended.md), relation_open (table access and permissions)
  - [systable_beginscan](../s/systable_beginscan.md), systable_getnext (catalog scanning)
  - [CatalogTupleInsert](CatalogTupleInsert.md), heap_form_tuple (catalog modifications)
  - [recordDependencyOn](../r/recordDependencyOn.md), recordDependencyOnExpr, recordSharedDependencyOn (dependency tracking)
  - InvokeObjectPostCreateHook, CacheInvalidateRelcache (event hooks and cache management)
- Called from:
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (main utility command dispatcher)

## Notes and Other Information
- Requires AccessExclusiveLock on target table to prevent concurrent DDL operations
- Validates policy name uniqueness per table (duplicate names across different tables are allowed)
- INSERT policies can only have WITH CHECK clauses, not USING clauses
- SELECT and DELETE policies cannot have WITH CHECK clauses
- Does not create dependencies on the PUBLIC role as it's implicitly available
- Returns ObjectAddress of the created policy for use by dependency and event systems
- Supports both permissive (OR-ed) and restrictive (AND-ed) policy types introduced in PostgreSQL 10

## Simplified Source

```c
ObjectAddress
CreatePolicy(CreatePolicyStmt *stmt)
{
    Relation pg_policy_rel;
    Oid policy_id, table_id;
    Relation target_table;
    char polcmd;
    Datum *role_oids;
    int nitems = 0;
    ArrayType *role_ids;
    ParseState *qual_pstate, *with_check_pstate;
    ParseNamespaceItem *nsitem;
    Node *qual, *with_check_qual;
    HeapTuple policy_tuple;
    Datum values[Natts_pg_policy];
    bool isnull[Natts_pg_policy];
    ObjectAddress target, myself;

    // Parse command type (SELECT, INSERT, UPDATE, DELETE)
    polcmd = parse_policy_command(stmt->cmd_name);

    // Validate command/clause combinations
    if ((polcmd == ACL_SELECT_CHR || polcmd == ACL_DELETE_CHR) && stmt->with_check != NULL) {
        ereport(ERROR, "WITH CHECK cannot be applied to SELECT or DELETE");
    }

    if (polcmd == ACL_INSERT_CHR && stmt->qual != NULL) {
        ereport(ERROR, "only WITH CHECK expression allowed for INSERT");
    }

    // Convert role list to array
    role_oids = policy_role_list_to_array(stmt->roles, &nitems);
    role_ids = construct_array_builtin(role_oids, nitems, OIDOID);

    // Create parse states for expression processing
    qual_pstate = make_parsestate(NULL);
    with_check_pstate = make_parsestate(NULL);

    memset(values, 0, sizeof(values));
    memset(isnull, 0, sizeof(isnull));

    // Get table ID and handle permissions
    table_id = RangeVarGetRelidExtended(stmt->table, AccessExclusiveLock, 0,
                                       RangeVarCallbackForPolicy, (void *) stmt);

    target_table = relation_open(table_id, NoLock);

    // Set up parse states for both USING and WITH CHECK clauses
    nsitem = addRangeTableEntryForRelation(qual_pstate, target_table,
                                          AccessShareLock, NULL, false, false);
    addNSItemToQuery(qual_pstate, nsitem, false, true, true);

    nsitem = addRangeTableEntryForRelation(with_check_pstate, target_table,
                                          AccessShareLock, NULL, false, false);
    addNSItemToQuery(with_check_pstate, nsitem, false, true, true);

    // Transform USING and WITH CHECK clauses
    qual = transformWhereClause(qual_pstate, stmt->qual, EXPR_KIND_POLICY, "POLICY");
    with_check_qual = transformWhereClause(with_check_pstate, stmt->with_check,
                                          EXPR_KIND_POLICY, "POLICY");

    // Fix collation information
    assign_expr_collations(qual_pstate, qual);
    assign_expr_collations(with_check_pstate, with_check_qual);

    // Open policy catalog
    pg_policy_rel = table_open(PolicyRelationId, RowExclusiveLock);

    // Check for duplicate policy name on this table
    // (Implementation uses systable_beginscan with table_id and policy_name)

    // Generate new policy OID and prepare values
    policy_id = GetNewOidWithIndex(pg_policy_rel, PolicyOidIndexId, Anum_pg_policy_oid);
    values[Anum_pg_policy_oid - 1] = ObjectIdGetDatum(policy_id);
    values[Anum_pg_policy_polrelid - 1] = ObjectIdGetDatum(table_id);
    values[Anum_pg_policy_polname - 1] = DirectFunctionCall1(namein,
                                                            CStringGetDatum(stmt->policy_name));
    values[Anum_pg_policy_polcmd - 1] = CharGetDatum(polcmd);
    values[Anum_pg_policy_polpermissive - 1] = BoolGetDatum(stmt->permissive);
    values[Anum_pg_policy_polroles - 1] = PointerGetDatum(role_ids);

    // Add USING clause if present
    if (qual)
        values[Anum_pg_policy_polqual - 1] = CStringGetTextDatum(nodeToString(qual));
    else
        isnull[Anum_pg_policy_polqual - 1] = true;

    // Add WITH CHECK clause if present
    if (with_check_qual)
        values[Anum_pg_policy_polwithcheck - 1] = CStringGetTextDatum(nodeToString(with_check_qual));
    else
        isnull[Anum_pg_policy_polwithcheck - 1] = true;

    // Insert catalog entry
    policy_tuple = heap_form_tuple(RelationGetDescr(pg_policy_rel), values, isnull);
    CatalogTupleInsert(pg_policy_rel, policy_tuple);

    // Record dependencies
    myself.classId = PolicyRelationId;
    myself.objectId = policy_id;
    myself.objectSubId = 0;

    target.classId = RelationRelationId;
    target.objectId = table_id;
    target.objectSubId = 0;
    recordDependencyOn(&myself, &target, DEPENDENCY_AUTO);

    // Record dependencies on expressions and roles
    recordDependencyOnExpr(&myself, qual, qual_pstate->p_rtable, DEPENDENCY_NORMAL);
    recordDependencyOnExpr(&myself, with_check_qual, with_check_pstate->p_rtable, DEPENDENCY_NORMAL);

    for (int i = 0; i < nitems; i++) {
        target.classId = AuthIdRelationId;
        target.objectId = DatumGetObjectId(role_oids[i]);
        target.objectSubId = 0;
        if (target.objectId != ACL_ID_PUBLIC) {
            recordSharedDependencyOn(&myself, &target, SHARED_DEPENDENCY_POLICY);
        }
    }

    // Post-creation hooks and cleanup
    InvokeObjectPostCreateHook(PolicyRelationId, policy_id, 0);
    CacheInvalidateRelcache(target_table);

    // Cleanup
    heap_freetuple(policy_tuple);
    free_parsestate(qual_pstate);
    free_parsestate(with_check_pstate);
    relation_close(target_table, NoLock);
    table_close(pg_policy_rel, RowExclusiveLock);

    return myself;
}
```