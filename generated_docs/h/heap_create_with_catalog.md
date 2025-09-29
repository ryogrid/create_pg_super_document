# heap_create_with_catalog

## Location
[src/backend/catalog/heap.c:1105-1525](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L1105-L1525)

## Overview
Creates a new relation with a complete catalog entry in PostgreSQL's system catalogs, handling both the physical storage creation and all associated metadata registration.

## Definition

```c
enumber = InvalidRelFileNumber;
```
## Detailed Description
This function is the comprehensive interface for creating new relations in PostgreSQL. It handles the complete process of creating a relation from start to finish, including:

1. **Validation and Conflict Checking**: Validates the tuple descriptor, checks for existing relations and types with conflicting names, and handles array type name conflicts through moveArrayTypeName.

2. **OID Assignment**: Allocates OIDs for the relation and handles binary upgrade scenarios where specific OIDs may be pre-assigned.

3. **Physical Storage Creation**: Creates the actual heap storage through heap_create() and establishes the relcache entry.

4. **Type System Integration**: For most relation kinds (excluding sequences, toast tables, and indexes), creates corresponding PostgreSQL composite types and array types through AddNewRelationType and TypeCreate.

5. **Catalog Registration**: Registers the relation in pg_class via AddNewRelationTuple and creates attribute entries in pg_attribute via AddNewAttributeTuples.

6. **Dependency Management**: Establishes proper dependency relationships with namespaces, owners, access methods, and extensions to ensure proper cascade behavior during drops.

7. **Constraint and Default Handling**: Processes any supplied constraints and defaults through StoreConstraints.

8. **Transaction Coordination**: Handles special commit actions for temporary tables and ensures proper locking throughout the process.

## Parameters / Member Variables
- : Name of the new relation
- : OID of the namespace where the relation will be created
- : OID of the tablespace for physical storage
- : Specific OID to assign to the relation, or InvalidOid for automatic assignment
- : OID for the relation's row type, or InvalidOid for automatic assignment
- : For typed tables, the OID of the underlying composite type
- : OID of the relation's owner (user/role)
- : OID of the access method to use for the relation
- : Tuple descriptor defining the relation's column structure
- : List of pre-processed CHECK constraints and column defaults
- : Character indicating the kind of relation (table, view, index, etc.)
- : Persistence characteristic (permanent, temporary, or unlogged)
- : Whether this is a shared system relation
- : Whether the relation uses the relfilenumber mapping system
- : ON COMMIT action for temporary tables
- : Relation options in Datum form
- : Whether to apply user-defined default ACL permissions
- : Whether to allow creation in system namespaces
- : Whether this is an internal system-generated relation
- : OID used for tracking relation rewrites
- : Output parameter receiving the object address of the created pg_type entry

## Dependencies
- Functions called/Symbols referenced:
  - [heap_create](heap_create.md) (creates physical storage and relcache entry)
  - [AddNewRelationType](../A/AddNewRelationType.md) (creates composite type for relation)
  - [TypeCreate](../T/TypeCreate.md) (creates array type over composite type)
  - [AddNewRelationTuple](../A/AddNewRelationTuple.md) (registers relation in pg_class)
  - [AddNewAttributeTuples](../A/AddNewAttributeTuples.md) (creates pg_attribute entries)
  - [moveArrayTypeName](../m/moveArrayTypeName.md) (handles array type name conflicts)
  - [StoreConstraints](../S/StoreConstraints.md) (processes constraints and defaults)
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md) (establishes ownership dependencies)
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md) (handles extension membership)
  - [register_on_commit_action](../r/register_on_commit_action.md) (handles temporary table commit actions)
- Called from (representative examples):
  - [DefineRelation](../D/DefineRelation.md) (main table creation path)
  - [create_toast_table](../c/create_toast_table.md) (TOAST table creation)
  - [make_new_heap](../m/make_new_heap.md) (table clustering/rewriting)

## Notes and Other Information
- This function is central to PostgreSQL's DDL operations and is used whenever a new cataloged relation needs to be created
- The function handles binary upgrade scenarios specially, using pre-assigned OIDs to maintain consistency during pg_upgrade operations
- For relations that get composite types (most relations except sequences, toast tables, and indexes), both a composite type and its corresponding array type are created
- The function establishes comprehensive dependency tracking to ensure proper cleanup during DROP operations
- Access method dependencies are only recorded for relations that actually use table access methods
- The function requires either normal processing mode or bootstrap mode - it cannot be called during other initialization phases
- Lock acquisition on the new relation OID prevents race conditions with concurrent DDL operations

## Simplified Source

```c
Oid heap_create_with_catalog(const char *relname,
                            Oid relnamespace,
                            Oid reltablespace,
                            Oid relid,
                            Oid reltypeid,
                            Oid reloftypeid,
                            Oid ownerid,
                            Oid accessmtd,
                            TupleDesc tupdesc,
                            List *cooked_constraints,
                            char relkind,
                            char relpersistence,
                            bool shared_relation,
                            bool mapped_relation,
                            OnCommitAction oncommit,
                            Datum reloptions,
                            bool use_user_acl,
                            bool allow_system_table_mods,
                            bool is_internal,
                            Oid relrewrite,
                            ObjectAddress *typaddress) {

    Relation pg_class_desc;
    Relation new_rel_desc;
    Acl *relacl;
    Oid existing_relid;
    Oid old_type_oid;
    Oid new_type_oid;
    RelFileNumber relfilenumber = InvalidRelFileNumber;
    TransactionId relfrozenxid;
    MultiXactId relminmxid;

    // Open pg_class catalog for updates
    pg_class_desc = table_open(RelationRelationId, RowExclusiveLock);

    // Validate tuple descriptor for the relation kind
    CheckAttributeNamesTypes(tupdesc, relkind,
                           allow_system_table_mods ? CHKATYPE_ANYARRAY : 0);

    // Check if relation already exists
    existing_relid = get_relname_relid(relname, relnamespace);
    if (existing_relid != InvalidOid)
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_TABLE),
                       errmsg("relation \"%s\" already exists", relname)));

    // Handle type name conflicts for composite types
    old_type_oid = GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid,
                                  CStringGetDatum(relname),
                                  ObjectIdGetDatum(relnamespace));
    if (OidIsValid(old_type_oid)) {
        if (!moveArrayTypeName(old_type_oid, relname, relnamespace))
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                           errmsg("type \"%s\" already exists", relname)));
    }

    // Validate shared relation tablespace
    if (shared_relation && reltablespace != GLOBALTABLESPACE_OID)
        elog(ERROR, "shared relations must be placed in pg_global tablespace");

    // Assign OID for the relation
    if (!OidIsValid(relid)) {
        // Handle binary upgrade mode with pre-assigned OIDs
        if (IsBinaryUpgrade) {
            if (relkind == RELKIND_TOASTVALUE) {
                // Use toast-specific binary upgrade OIDs
                relid = binary_upgrade_next_toast_pg_class_oid;
                relfilenumber = binary_upgrade_next_toast_pg_class_relfilenumber;
            } else {
                // Use heap-specific binary upgrade OIDs
                relid = binary_upgrade_next_heap_pg_class_oid;
                if (RELKIND_HAS_STORAGE(relkind))
                    relfilenumber = binary_upgrade_next_heap_pg_class_relfilenumber;
            }
        }

        if (!OidIsValid(relid))
            relid = GetNewRelFileNumber(reltablespace, pg_class_desc, relpersistence);
    }

    // Lock the relation OID to prevent concurrent access
    LockRelationOid(relid, AccessExclusiveLock);

    // Determine initial permissions based on relation kind
    if (use_user_acl) {
        switch (relkind) {
            case RELKIND_RELATION:
            case RELKIND_VIEW:
            case RELKIND_MATVIEW:
            case RELKIND_FOREIGN_TABLE:
            case RELKIND_PARTITIONED_TABLE:
                relacl = get_user_default_acl(OBJECT_TABLE, ownerid, relnamespace);
                break;
            case RELKIND_SEQUENCE:
                relacl = get_user_default_acl(OBJECT_SEQUENCE, ownerid, relnamespace);
                break;
            default:
                relacl = NULL;
                break;
        }
    } else {
        relacl = NULL;
    }

    // Create the physical relation and relcache entry
    new_rel_desc = heap_create(relname, relnamespace, reltablespace,
                              relid, relfilenumber, accessmtd, tupdesc,
                              relkind, relpersistence, shared_relation,
                              mapped_relation, allow_system_table_mods,
                              &relfrozenxid, &relminmxid, true);

    new_rel_desc->rd_rel->relrewrite = relrewrite;

    // Create composite type for most relation kinds (not sequences, toast, indexes)
    if (!(relkind == RELKIND_SEQUENCE ||
          relkind == RELKIND_TOASTVALUE ||
          relkind == RELKIND_INDEX ||
          relkind == RELKIND_PARTITIONED_INDEX)) {

        // Create array type OID first
        Oid new_array_oid = AssignTypeArrayOid();

        // Create the composite type
        ObjectAddress new_type_addr = AddNewRelationType(relname, relnamespace,
                                                        relid, relkind, ownerid,
                                                        reltypeid, new_array_oid);
        new_type_oid = new_type_addr.objectId;
        if (typaddress)
            *typaddress = new_type_addr;

        // Create the corresponding array type
        char *relarrayname = makeArrayTypeName(relname, relnamespace);
        TypeCreate(new_array_oid, relarrayname, relnamespace, InvalidOid, 0,
                  ownerid, -1, TYPTYPE_BASE, TYPCATEGORY_ARRAY, false,
                  DEFAULT_TYPDELIM, F_ARRAY_IN, F_ARRAY_OUT, F_ARRAY_RECV,
                  F_ARRAY_SEND, InvalidOid, InvalidOid, F_ARRAY_TYPANALYZE,
                  F_ARRAY_SUBSCRIPT_HANDLER, new_type_oid, true, InvalidOid,
                  InvalidOid, NULL, NULL, false, TYPALIGN_DOUBLE,
                  TYPSTORAGE_EXTENDED, -1, 0, false, InvalidOid);
        pfree(relarrayname);
    } else {
        new_type_oid = InvalidOid;
    }

    // Register relation in pg_class catalog
    AddNewRelationTuple(pg_class_desc, new_rel_desc, relid, new_type_oid,
                       reloftypeid, ownerid, relkind, relfrozenxid,
                       relminmxid, PointerGetDatum(relacl), reloptions);

    // Create attribute entries in pg_attribute
    AddNewAttributeTuples(relid, new_rel_desc->rd_att, relkind);

    // Establish dependency relationships (except for composite types and toast tables)
    if (relkind != RELKIND_COMPOSITE_TYPE &&
        relkind != RELKIND_TOASTVALUE &&
        !IsBootstrapProcessingMode()) {

        ObjectAddress myself;
        ObjectAddressSet(myself, RelationRelationId, relid);

        // Record dependencies on owner, ACL, extension, namespace
        recordDependencyOnOwner(RelationRelationId, relid, ownerid);
        recordDependencyOnNewAcl(RelationRelationId, relid, 0, ownerid, relacl);
        recordDependencyOnCurrentExtension(&myself, false);

        // Create dependency list for namespace and access method
        ObjectAddresses *addrs = new_object_addresses();
        ObjectAddress referenced;

        ObjectAddressSet(referenced, NamespaceRelationId, relnamespace);
        add_exact_object_address(&referenced, addrs);

        if (reloftypeid) {
            ObjectAddressSet(referenced, TypeRelationId, reloftypeid);
            add_exact_object_address(&referenced, addrs);
        }

        if ((RELKIND_HAS_TABLE_AM(relkind) && relkind != RELKIND_TOASTVALUE) ||
            (relkind == RELKIND_PARTITIONED_TABLE && OidIsValid(accessmtd))) {
            ObjectAddressSet(referenced, AccessMethodRelationId, accessmtd);
            add_exact_object_address(&referenced, addrs);
        }

        record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL);
        free_object_addresses(addrs);
    }

    // Invoke post-creation hook
    InvokeObjectPostCreateHookArg(RelationRelationId, relid, 0, is_internal);

    // Store constraints and defaults
    StoreConstraints(new_rel_desc, cooked_constraints, is_internal);

    // Register special commit actions for temporary tables
    if (oncommit != ONCOMMIT_NOOP)
        register_on_commit_action(relid, oncommit);

    // Close relations and return the new relation OID
    table_close(new_rel_desc, NoLock);
    table_close(pg_class_desc, RowExclusiveLock);

    return relid;
}
```