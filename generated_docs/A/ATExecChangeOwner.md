# ATExecChangeOwner

## Location
[src/backend/commands/tablecmds.c:14476-14716](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L14476-L14716)

## Overview
ATExecChangeOwner implements the ALTER TABLE OWNER command, changing the ownership of tables, views, indexes, sequences, and related objects while handling permission checks, ACL updates, and recursive ownership changes for dependent objects.

## Definition
```c
void
ATExecChangeOwner(Oid relationOid, Oid newOwnerId, bool recursing, LOCKMODE lockmode)
```

## Detailed Description
This function handles ownership changes for various types of relations in PostgreSQL. It performs comprehensive validation of the target relation type, enforces permission requirements, and updates both the relation's owner and its access control lists. The function handles special cases for different relation kinds, including preventing direct ownership changes of indexes (suggesting users change the table owner instead) and sequences owned by tables.

When changing ownership, the function updates the pg_class catalog, adjusts ACLs for both the relation and its columns, updates dependency information, and recursively changes ownership of related objects like indexes, toast tables, sequences, and row types. The recursing parameter allows the function to skip permission checks when called recursively for dependent objects.

## Parameters
- `relationOid`: OID of the relation whose ownership is being changed
- `newOwnerId`: OID of the new owner
- `recursing`: True when recursively changing ownership of dependent objects (skips permission checks)
- `lockmode`: Lock mode to use when opening the relation

## Dependencies
- Functions called/Symbols referenced:
  - [relation_open](../r/relation_open.md), relation_close, table_open, table_close
  - [SearchSysCache1](../S/SearchSysCache1.md), ReleaseSysCache, SysCacheGetAttr
  - [superuser](../s/superuser.md), object_ownercheck, check_can_set_role, object_aclcheck
  - [aclnewowner](../a/aclnewowner.md), change_owner_fix_column_acls, changeDependencyOnOwner
  - [AlterTypeOwnerInternal](AlterTypeOwnerInternal.md), RelationGetIndexList
  - [change_owner_recurse_to_sequences](../c/change_owner_recurse_to_sequences.md)
  - [sequenceIsOwned](../s/sequenceIsOwned.md), errdetail_relkind_not_supported
  - [heap_modify_tuple](../h/heap_modify_tuple.md), CatalogTupleUpdate, heap_freetuple
  - InvokeObjectPostAlterHook
- Called from:
  - [ATExecCmd](ATExecCmd.md) (main ALTER TABLE command execution)
  - [shdepReassignOwned_Owner](../s/shdepReassignOwned_Owner.md) (during REASSIGN OWNED operations)
  - [AlterTypeOwner_oid](AlterTypeOwner_oid.md) (for composite types)
  - [change_owner_recurse_to_sequences](../c/change_owner_recurse_to_sequences.md) (for dependent sequences)
  - Recursively calls itself for indexes and toast tables

## Notes and Other Information
- Supports tables, views, materialized views, foreign tables, and partitioned tables
- Prevents direct ownership changes of indexes and partitioned indexes when not recursing
- Handles owned sequences by preventing ownership changes when they're linked to tables
- Updates column-level ACLs in addition to relation-level ACLs
- Recursively changes ownership of indexes, toast tables, and dependent sequences
- Also changes ownership of the relation's row type if it exists
- Fires post-alter hooks for proper event notification
- Uses appropriate error messages and hints for unsupported operations

## Simplified Source

```c
void
ATExecChangeOwner(Oid relationOid, Oid newOwnerId, bool recursing, LOCKMODE lockmode) {
    Relation target_rel, class_rel;
    HeapTuple tuple;
    Form_pg_class tuple_class;

    // Open the target relation and get its pg_class entry
    target_rel = relation_open(relationOid, lockmode);
    class_rel = table_open(RelationRelationId, RowExclusiveLock);
    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relationOid));
    tuple_class = (Form_pg_class) GETSTRUCT(tuple);

    // Validate that we can change ownership of this relation type
    switch (tuple_class->relkind) {
        case RELKIND_RELATION:
        case RELKIND_VIEW:
        case RELKIND_MATVIEW:
        case RELKIND_FOREIGN_TABLE:
        case RELKIND_PARTITIONED_TABLE:
            // These types can have ownership changed
            break;
        case RELKIND_INDEX:
            if (!recursing) {
                // Warn but don't error for indexes (backward compatibility)
                if (tuple_class->relowner != newOwnerId)
                    ereport(WARNING, (errmsg("cannot change owner of index \"%s\"",
                                           NameStr(tuple_class->relname)),
                                    errhint("Change the ownership of the index's table instead.")));
                newOwnerId = tuple_class->relowner; // No-op
            }
            break;
        case RELKIND_SEQUENCE:
            // Prevent changing ownership of sequences owned by tables
            if (!recursing && tuple_class->relowner != newOwnerId) {
                Oid tableId;
                int32 colId;
                if (sequenceIsOwned(relationOid, DEPENDENCY_AUTO, &tableId, &colId) ||
                    sequenceIsOwned(relationOid, DEPENDENCY_INTERNAL, &tableId, &colId))
                    ereport(ERROR, (errmsg("cannot change owner of sequence \"%s\"",
                                         NameStr(tuple_class->relname)),
                                  errdetail("Sequence is linked to table.")));
            }
            break;
        default:
            // Other types not supported
            ereport(ERROR, (errmsg("cannot change owner of relation \"%s\"",
                                 NameStr(tuple_class->relname))));
    }

    // Perform the ownership change if needed
    if (tuple_class->relowner != newOwnerId) {
        // Check permissions (unless recursing)
        if (!recursing) {
            if (!superuser()) {
                if (!object_ownercheck(RelationRelationId, relationOid, GetUserId()))
                    aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(tuple_class->relkind),
                                 RelationGetRelationName(target_rel));
                check_can_set_role(GetUserId(), newOwnerId);

                // New owner needs CREATE privilege on namespace
                AclResult aclresult = object_aclcheck(NamespaceRelationId,
                                                    tuple_class->relnamespace,
                                                    newOwnerId, ACL_CREATE);
                if (aclresult != ACLCHECK_OK)
                    aclcheck_error(aclresult, OBJECT_SCHEMA,
                                 get_namespace_name(tuple_class->relnamespace));
            }
        }

        // Update pg_class row with new owner
        Datum repl_val[Natts_pg_class] = {0};
        bool repl_null[Natts_pg_class] = {0};
        bool repl_repl[Natts_pg_class] = {0};

        repl_repl[Anum_pg_class_relowner - 1] = true;
        repl_val[Anum_pg_class_relowner - 1] = ObjectIdGetDatum(newOwnerId);

        // Update ACL if present
        Datum aclDatum;
        bool isNull;
        aclDatum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_relacl, &isNull);
        if (!isNull) {
            Acl *newAcl = aclnewowner(DatumGetAclP(aclDatum),
                                     tuple_class->relowner, newOwnerId);
            repl_repl[Anum_pg_class_relacl - 1] = true;
            repl_val[Anum_pg_class_relacl - 1] = PointerGetDatum(newAcl);
        }

        HeapTuple newtuple = heap_modify_tuple(tuple, RelationGetDescr(class_rel),
                                              repl_val, repl_null, repl_repl);
        CatalogTupleUpdate(class_rel, &newtuple->t_self, newtuple);
        heap_freetuple(newtuple);

        // Update column ACLs and dependencies
        change_owner_fix_column_acls(relationOid, tuple_class->relowner, newOwnerId);
        if (tuple_class->relkind != RELKIND_COMPOSITE_TYPE &&
            tuple_class->relkind != RELKIND_INDEX &&
            tuple_class->relkind != RELKIND_PARTITIONED_INDEX &&
            tuple_class->relkind != RELKIND_TOASTVALUE)
            changeDependencyOnOwner(RelationRelationId, relationOid, newOwnerId);

        // Change ownership of row type if it exists
        if (OidIsValid(tuple_class->reltype))
            AlterTypeOwnerInternal(tuple_class->reltype, newOwnerId);

        // Recursively change ownership of related objects
        if (tuple_class->relkind == RELKIND_RELATION ||
            tuple_class->relkind == RELKIND_PARTITIONED_TABLE ||
            tuple_class->relkind == RELKIND_MATVIEW ||
            tuple_class->relkind == RELKIND_TOASTVALUE) {

            // Change ownership of indexes
            List *index_oid_list = RelationGetIndexList(target_rel);
            foreach(i, index_oid_list)
                ATExecChangeOwner(lfirst_oid(i), newOwnerId, true, lockmode);
            list_free(index_oid_list);
        }

        // Change ownership of toast table and dependent sequences
        if (tuple_class->reltoastrelid != InvalidOid)
            ATExecChangeOwner(tuple_class->reltoastrelid, newOwnerId, true, lockmode);
        change_owner_recurse_to_sequences(relationOid, newOwnerId, lockmode);
    }

    InvokeObjectPostAlterHook(RelationRelationId, relationOid, 0);

    ReleaseSysCache(tuple);
    table_close(class_rel, RowExclusiveLock);
    relation_close(target_rel, NoLock);
}
```