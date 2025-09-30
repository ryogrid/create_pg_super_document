# ExecGrant_Relation

## Location
[src/backend/catalog/aclchk.c:1825-2155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L1825-L2155)

## Overview
Processes GRANT/REVOKE operations on relations (tables, sequences), handling both relation-level and column-level privileges with comprehensive validation and ACL management.

## Definition
```c
static void ExecGrant_Relation(InternalGrant *istmt)
```

## Detailed Description
This static function is the main workhorse for processing GRANT/REVOKE operations on relations, including both regular tables and sequences. It handles complex privilege management scenarios including:

1. **Object Validation**: Validates that the target objects are appropriate for GRANT operations (not indexes or composite types)
2. **Privilege Type Handling**: Adjusts privilege types based on object kind (sequence vs. table) and validates privilege compatibility
3. **Column Privilege Processing**: Handles both explicit column privileges and implicit column privilege revocation during relation-level REVOKE operations
4. **ACL Management**: Creates, updates, and manages Access Control Lists for both relation-level and column-level privileges
5. **Dependency Tracking**: Updates shared dependency information to track role relationships with privileges
6. **Catalog Updates**: Updates both pg_class (for relation ACLs) and pg_attribute (for column ACLs) system catalogs

The function processes each relation in the istmt->objects list, handling relation-level privileges first, then processing any column-specific privileges. It includes extensive validation and error handling for various edge cases and object type combinations.

## Parameters
- `istmt`: InternalGrant structure containing complete details of the grant/revoke operation including:
  - Object list, privilege specifications, grantees, grant options, and operation type

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [SearchSysCacheLocked1](../S/SearchSysCacheLocked1.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [acldefault](../a/acldefault.md)
  - DatumGetAclPCopy
  - [aclmembers](../a/aclmembers.md)
  - [aclcopy](../a/aclcopy.md)
  - [select_best_grantor](../s/select_best_grantor.md)
  - [restrict_and_check_grant](../r/restrict_and_check_grant.md)
  - [merge_acl_with_grant](../m/merge_acl_with_grant.md)
  - [expand_all_col_privileges](../e/expand_all_col_privileges.md)
  - [expand_col_privileges](../e/expand_col_privileges.md)
  - [ExecGrant_Attribute](ExecGrant_Attribute.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [UnlockTuple](../U/UnlockTuple.md)
  - [recordExtensionInitPriv](../r/recordExtensionInitPriv.md)
  - [updateAclDependencies](../u/updateAclDependencies.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
- Called from (representative examples):
  - [ExecGrantStmt_oids](ExecGrantStmt_oids.md)

## Notes and Other Information
- This is a static function only used within aclchk.c as part of the grant/revoke processing pipeline
- Handles both tables and sequences with different privilege sets (sequences support USAGE, SELECT, UPDATE)
- Implements SQL standard requirement that REVOKE on relation-level privileges also revokes corresponding column-level privileges
- Includes backward compatibility warnings for invalid privilege types on sequences when using generic TABLE syntax
- Manages complex column privilege arrays indexed from FirstLowInvalidHeapAttributeNumber to handle system columns
- Performs optimization to avoid unnecessary catalog updates when privileges don't actually change
- Uses tuple locking mechanisms to ensure consistency during concurrent operations
- Validates that column privileges are appropriate for the object type (sequences only support SELECT on columns)
- Records initial privileges for extension objects to support proper privilege restoration during upgrades

## Simplified Source

```c
static void
ExecGrant_Relation(InternalGrant *istmt)
{
    Relation relation, attRelation;
    ListCell *cell;

    // Open catalogs for relation and attribute privileges
    relation = table_open(RelationRelationId, RowExclusiveLock);
    attRelation = table_open(AttributeRelationId, RowExclusiveLock);

    // Process each relation
    foreach(cell, istmt->objects)
    {
        Oid relOid = lfirst_oid(cell);
        Form_pg_class pg_class_tuple;
        AclMode this_privileges;
        AclMode *col_privileges;
        Acl *old_acl, *old_rel_acl;
        Oid ownerId;
        HeapTuple tuple;
        bool have_col_privileges = false;

        // Look up relation metadata
        tuple = SearchSysCacheLocked1(RELOID, ObjectIdGetDatum(relOid));
        if (!HeapTupleIsValid(tuple))
            elog(ERROR, "cache lookup failed for relation %u", relOid);
        pg_class_tuple = (Form_pg_class) GETSTRUCT(tuple);

        // Validate object type - reject indexes and composite types
        if (pg_class_tuple->relkind == RELKIND_INDEX ||
            pg_class_tuple->relkind == RELKIND_PARTITIONED_INDEX)
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                           errmsg("\"%s\" is an index", NameStr(pg_class_tuple->relname))));
        if (pg_class_tuple->relkind == RELKIND_COMPOSITE_TYPE)
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                           errmsg("\"%s\" is a composite type", NameStr(pg_class_tuple->relname))));

        // Set appropriate privilege defaults based on object type
        if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS)
        {
            if (pg_class_tuple->relkind == RELKIND_SEQUENCE)
                this_privileges = ACL_ALL_RIGHTS_SEQUENCE;
            else
                this_privileges = ACL_ALL_RIGHTS_RELATION;
        }
        else
            this_privileges = istmt->privileges;

        // Initialize column privileges array
        num_col_privileges = pg_class_tuple->relnatts - FirstLowInvalidHeapAttributeNumber + 1;
        col_privileges = (AclMode *) palloc0(num_col_privileges * sizeof(AclMode));

        // Handle implicit column privilege revocation
        if (!istmt->is_grant && (this_privileges & ACL_ALL_RIGHTS_COLUMN) != 0)
        {
            expand_all_col_privileges(relOid, pg_class_tuple,
                                      this_privileges & ACL_ALL_RIGHTS_COLUMN,
                                      col_privileges, num_col_privileges);
            have_col_privileges = true;
        }

        // Get existing relation ACL
        ownerId = pg_class_tuple->relowner;
        aclDatum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_relacl, &isNull);
        if (isNull)
        {
            if (pg_class_tuple->relkind == RELKIND_SEQUENCE)
                old_acl = acldefault(OBJECT_SEQUENCE, ownerId);
            else
                old_acl = acldefault(OBJECT_TABLE, ownerId);
        }
        else
            old_acl = DatumGetAclPCopy(aclDatum);

        old_rel_acl = aclcopy(old_acl);

        // Process relation-level privileges
        if (this_privileges != ACL_NO_RIGHTS)
        {
            AclMode avail_goptions;
            Acl *new_acl;
            Oid grantorId;
            ObjectType objtype;

            // Determine grantor and object type
            select_best_grantor(GetUserId(), this_privileges, old_acl, ownerId,
                                &grantorId, &avail_goptions);
            objtype = (pg_class_tuple->relkind == RELKIND_SEQUENCE) ?
                      OBJECT_SEQUENCE : OBJECT_TABLE;

            // Validate and restrict privileges
            this_privileges = restrict_and_check_grant(istmt->is_grant, avail_goptions,
                                                       istmt->all_privs, this_privileges,
                                                       relOid, grantorId, objtype,
                                                       NameStr(pg_class_tuple->relname), 0, NULL);

            // Generate and update new ACL
            new_acl = merge_acl_with_grant(old_acl, istmt->is_grant,
                                           istmt->grant_option, istmt->behavior,
                                           istmt->grantees, this_privileges,
                                           grantorId, ownerId);

            // Update catalog
            replaces[Anum_pg_class_relacl - 1] = true;
            values[Anum_pg_class_relacl - 1] = PointerGetDatum(new_acl);
            newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation),
                                         values, nulls, replaces);
            CatalogTupleUpdate(relation, &newtuple->t_self, newtuple);
            UnlockTuple(relation, &tuple->t_self, InplaceUpdateTupleLock);

            // Update dependencies and extensions
            recordExtensionInitPriv(relOid, RelationRelationId, 0, new_acl);
            updateAclDependencies(RelationRelationId, relOid, 0, ownerId,
                                  noldmembers, oldmembers, nnewmembers, newmembers);
            pfree(new_acl);
        }
        else
            UnlockTuple(relation, &tuple->t_self, InplaceUpdateTupleLock);

        // Process column-level privileges
        foreach(cell_colprivs, istmt->col_privs)
        {
            AccessPriv *col_privs = (AccessPriv *) lfirst(cell_colprivs);

            // Determine column privilege type
            if (col_privs->priv_name == NULL)
                this_privileges = ACL_ALL_RIGHTS_COLUMN;
            else
                this_privileges = string_to_privilege(col_privs->priv_name);

            // Expand privileges to affected columns
            expand_col_privileges(col_privs->cols, relOid, this_privileges,
                                  col_privileges, num_col_privileges);
            have_col_privileges = true;
        }

        // Apply column privilege changes
        if (have_col_privileges)
        {
            for (i = 0; i < num_col_privileges; i++)
            {
                if (col_privileges[i] != ACL_NO_RIGHTS)
                    ExecGrant_Attribute(istmt, relOid, NameStr(pg_class_tuple->relname),
                                        i + FirstLowInvalidHeapAttributeNumber,
                                        ownerId, col_privileges[i],
                                        attRelation, old_rel_acl);
            }
        }

        pfree(old_rel_acl);
        pfree(col_privileges);
        ReleaseSysCache(tuple);
        CommandCounterIncrement();
    }

    table_close(attRelation, RowExclusiveLock);
    table_close(relation, RowExclusiveLock);
}
```