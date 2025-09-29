# DefineRelation

## Location
[src/backend/commands/tablecmds.c:698-1290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L698-L1290)

## Overview
DefineRelation is the core function for creating new relations (tables, views, indexes, etc.) in PostgreSQL, handling the complete process from parsing CREATE statements to catalog registration.

## Definition

```c
structively
	 * modified by MergeAttributes.)
	 */
	stmt->tableElts =
		MergeAttributes(stmt->tableElts, inheritOids,
						stmt->relation->relpersistence,
						stmt->partbound != NULL,
						&old_constraints);
```
## Detailed Description
DefineRelation serves as the primary entry point for creating database relations in PostgreSQL. It processes CREATE TABLE statements and related commands, coordinating the entire relation creation workflow. The function handles schema validation, inheritance processing, constraint management, partitioning setup, and catalog registration. It operates by first validating the creation parameters, processing inheritance relationships, building the relation descriptor, creating the physical relation through heap_create_with_catalog, and finally setting up any additional features like partitioning, indexes, and constraints.

## Parameters / Member Variables
- : CreateStmt parse tree containing the parsed CREATE TABLE statement with all table definition elements
- : Character indicating the relation type (RELKIND_RELATION for tables, RELKIND_VIEW for views, etc.)
- : Object identifier of the relation owner, or InvalidOid to use current user
- : Optional output parameter to receive the address of the corresponding pg_type entry
- : Original SQL query string used for error reporting and context

## Dependencies
- Functions called/Symbols referenced:
  - [BuildDescForRelation](../B/BuildDescForRelation.md)
  - [heap_create_with_catalog](../h/heap_create_with_catalog.md)
  - [MergeAttributes](../M/MergeAttributes.md)
  - [RangeVarGetAndCheckCreationNamespace](../R/RangeVarGetAndCheckCreationNamespace.md)
  - [transformRelOptions](../t/transformRelOptions.md)
  - [view_reloptions](../v/view_reloptions.md)
  - [partitioned_table_reloptions](../p/partitioned_table_reloptions.md)
  - [heap_reloptions](../h/heap_reloptions.md)
  - [AddRelationNewConstraints](../A/AddRelationNewConstraints.md)
  - [StorePartitionBound](../S/StorePartitionBound.md)
  - [StoreCatalogInheritance](../S/StoreCatalogInheritance.md)
  - [relation_open](../r/relation_open.md)
  - [relation_close](../r/relation_close.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)
  - [DefineSequence](DefineSequence.md)
  - [DefineCompositeType](DefineCompositeType.md)
  - [DefineVirtualRelation](DefineVirtualRelation.md)
  - [create_ctas_internal](../c/create_ctas_internal.md)

## Notes and Other Information
DefineRelation is a complex function spanning nearly 600 lines that orchestrates the entire relation creation process. It performs extensive validation including permission checks, tablespace verification, and inheritance consistency. The function handles special cases for partitioned tables, temporary tables, and security-restricted operations. It processes both raw and cooked defaults/constraints, with raw expressions requiring later transformation after the relation exists. For partitioned tables, it sets up partition keys and validates partition bounds. When creating partitions, it automatically inherits indexes, triggers, and foreign key constraints from the parent table.

## Simplified Source
```c
/*
 * DefineRelation - Creates a new relation.
 *
 * stmt carries parsetree information from an ordinary CREATE TABLE statement.
 * The other arguments are used to extend the behavior for other cases:
 * relkind: relkind to assign to the new relation
 * ownerId: if not InvalidOid, use this as the new relation's owner.
 * typaddress: if not null, it's set to the pg_type entry's address.
 * queryString: for error reporting
 */
ObjectAddress
DefineRelation(CreateStmt *stmt, char relkind, Oid ownerId,
               ObjectAddress *typaddress, const char *queryString)
{
    char relname[NAMEDATALEN];
    Oid namespaceId;
    Oid relationId;
    Oid tablespaceId;
    Relation rel;
    TupleDesc descriptor;
    List *inheritOids;
    List *old_constraints;
    List *rawDefaults;
    List *cookedDefaults;
    Datum reloptions;
    ListCell *listptr;
    AttrNumber attnum;
    bool partitioned;
    static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
    Oid ofTypeId;
    ObjectAddress address;
    LOCKMODE parentLockmode;
    Oid accessMethodId = InvalidOid;

    /* Truncate relname to appropriate length */
    strlcpy(relname, stmt->relation->relname, NAMEDATALEN);

    /* Check consistency of arguments */
    if (stmt->oncommit != ONCOMMIT_NOOP &&
        stmt->relation->relpersistence != RELPERSISTENCE_TEMP)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                 errmsg("ON COMMIT can only be used on temporary tables")));

    if (stmt->partspec != NULL)
    {
        if (relkind != RELKIND_RELATION)
            elog(ERROR, "unexpected relkind: %d", (int) relkind);
        relkind = RELKIND_PARTITIONED_TABLE;
        partitioned = true;
    }
    else
        partitioned = false;

    /* Look up the namespace and check permissions */
    namespaceId = RangeVarGetAndCheckCreationNamespace(stmt->relation, NoLock, NULL);

    /* Security check for temp tables */
    if (stmt->relation->relpersistence == RELPERSISTENCE_TEMP &&
        InSecurityRestrictedOperation())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("cannot create temporary table within security-restricted operation")));

    /* Determine the lockmode for scanning parents */
    parentLockmode = (stmt->partbound != NULL ? AccessExclusiveLock :
                      ShareUpdateExclusiveLock);

    /* Process inheritance list */
    inheritOids = NIL;
    foreach(listptr, stmt->inhRelations)
    {
        RangeVar *rv = (RangeVar *) lfirst(listptr);
        Oid parentOid;

        parentOid = RangeVarGetRelid(rv, parentLockmode, false);

        /* Reject duplications in the list of parents */
        if (list_member_oid(inheritOids, parentOid))
            ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_TABLE),
                     errmsg("relation \"%s\" would be inherited from more than once",
                            get_rel_name(parentOid))));

        inheritOids = lappend_oid(inheritOids, parentOid);
    }

    /* Select tablespace */
    if (stmt->tablespacename)
    {
        tablespaceId = get_tablespace_oid(stmt->tablespacename, false);
        if (partitioned && tablespaceId == MyDatabaseTableSpace)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("cannot specify default tablespace for partitioned relations")));
    }
    else if (stmt->partbound)
    {
        Assert(list_length(inheritOids) == 1);
        tablespaceId = get_rel_tablespace(linitial_oid(inheritOids));
    }
    else
        tablespaceId = InvalidOid;

    /* Use default tablespace if needed */
    if (!OidIsValid(tablespaceId))
        tablespaceId = GetDefaultTablespace(stmt->relation->relpersistence,
                                            partitioned);

    /* Check tablespace permissions */
    if (OidIsValid(tablespaceId) && tablespaceId != MyDatabaseTableSpace)
    {
        AclResult aclresult = object_aclcheck(TableSpaceRelationId, tablespaceId,
                                              GetUserId(), ACL_CREATE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, OBJECT_TABLESPACE,
                           get_tablespace_name(tablespaceId));
    }

    /* Disallow placing user relations in pg_global */
    if (tablespaceId == GLOBALTABLESPACE_OID)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("only shared relations can be placed in pg_global tablespace")));

    /* Set owner */
    if (!OidIsValid(ownerId))
        ownerId = GetUserId();

    /* Parse and validate reloptions */
    reloptions = transformRelOptions((Datum) 0, stmt->options, NULL, validnsps,
                                     true, false);

    switch (relkind)
    {
        case RELKIND_VIEW:
            (void) view_reloptions(reloptions, true);
            break;
        case RELKIND_PARTITIONED_TABLE:
            (void) partitioned_table_reloptions(reloptions, true);
            break;
        default:
            (void) heap_reloptions(relkind, reloptions, true);
    }

    /* Handle OF type */
    if (stmt->ofTypename)
    {
        AclResult aclresult;
        ofTypeId = typenameTypeId(NULL, stmt->ofTypename);
        aclresult = object_aclcheck(TypeRelationId, ofTypeId, GetUserId(), ACL_USAGE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error_type(aclresult, ofTypeId);
    }
    else
        ofTypeId = InvalidOid;

    /* Process inheritance and merge attributes */
    stmt->tableElts = MergeAttributes(stmt->tableElts, inheritOids,
                                      stmt->relation->relpersistence,
                                      stmt->partbound != NULL,
                                      &old_constraints);

    /* Create tuple descriptor */
    descriptor = BuildDescForRelation(stmt->tableElts);

    /* Process column defaults */
    rawDefaults = NIL;
    cookedDefaults = NIL;
    attnum = 0;

    foreach(listptr, stmt->tableElts)
    {
        ColumnDef *colDef = lfirst(listptr);
        Form_pg_attribute attr;

        attnum++;
        attr = TupleDescAttr(descriptor, attnum - 1);

        if (colDef->raw_default != NULL)
        {
            RawColumnDefault *rawEnt = palloc(sizeof(RawColumnDefault));
            rawEnt->attnum = attnum;
            rawEnt->raw_default = colDef->raw_default;
            rawEnt->missingMode = false;
            rawEnt->generated = colDef->generated;
            rawDefaults = lappend(rawDefaults, rawEnt);
            attr->atthasdef = true;
        }
        else if (colDef->cooked_default != NULL)
        {
            CookedConstraint *cooked = palloc(sizeof(CookedConstraint));
            cooked->contype = CONSTR_DEFAULT;
            cooked->conoid = InvalidOid;
            cooked->name = NULL;
            cooked->attnum = attnum;
            cooked->expr = colDef->cooked_default;
            cooked->skip_validation = false;
            cooked->is_local = true;
            cooked->inhcount = 0;
            cooked->is_no_inherit = false;
            cookedDefaults = lappend(cookedDefaults, cooked);
            attr->atthasdef = true;
        }
    }

    /* Select access method */
    if (stmt->accessMethod != NULL)
    {
        Assert(RELKIND_HAS_TABLE_AM(relkind) || relkind == RELKIND_PARTITIONED_TABLE);
        accessMethodId = get_table_am_oid(stmt->accessMethod, false);
    }
    else if (RELKIND_HAS_TABLE_AM(relkind) || relkind == RELKIND_PARTITIONED_TABLE)
    {
        if (stmt->partbound)
        {
            Assert(list_length(inheritOids) == 1);
            accessMethodId = get_rel_relam(linitial_oid(inheritOids));
        }
        if (RELKIND_HAS_TABLE_AM(relkind) && !OidIsValid(accessMethodId))
            accessMethodId = get_table_am_oid(default_table_access_method, false);
    }

    /* Create the relation in the catalog */
    relationId = heap_create_with_catalog(relname,
                                          namespaceId,
                                          tablespaceId,
                                          InvalidOid,
                                          InvalidOid,
                                          ofTypeId,
                                          ownerId,
                                          accessMethodId,
                                          descriptor,
                                          list_concat(cookedDefaults,
                                                      old_constraints),
                                          relkind,
                                          stmt->relation->relpersistence,
                                          false,
                                          false,
                                          stmt->oncommit,
                                          reloptions,
                                          true,
                                          allowSystemTableMods,
                                          false,
                                          InvalidOid,
                                          typaddress);

    /* Make the new relation visible */
    CommandCounterIncrement();

    /* Open the new relation */
    rel = relation_open(relationId, AccessExclusiveLock);

    /* Add raw defaults and constraints */
    if (rawDefaults)
        AddRelationNewConstraints(rel, rawDefaults, NIL,
                                  true, true, false, queryString);

    CommandCounterIncrement();

    /* Process partition bound if this is a partition */
    if (stmt->partbound)
    {
        /* ... partition-specific processing ... */
        PartitionBoundSpec *bound;
        ParseState *pstate;
        Oid parentId = linitial_oid(inheritOids);
        Relation parent;

        parent = table_open(parentId, NoLock);

        if (parent->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                     errmsg("\"%s\" is not partitioned",
                            RelationGetRelationName(parent))));

        /* Transform and validate partition bound */
        pstate = make_parsestate(NULL);
        pstate->p_sourcetext = queryString;

        bound = transformPartitionBound(pstate, parent, stmt->partbound);
        check_new_partition_bound(relname, parent, bound, pstate);

        /* Store partition bound */
        StorePartitionBound(rel, parent, bound);
        table_close(parent, NoLock);
    }

    /* Store inheritance information */
    StoreCatalogInheritance(relationId, inheritOids, stmt->partbound != NULL);

    /* Process partitioning specification for partitioned tables */
    if (partitioned)
    {
        /* ... partitioning setup ... */
        ParseState *pstate = make_parsestate(NULL);
        pstate->p_sourcetext = queryString;

        stmt->partspec = transformPartitionSpec(rel, stmt->partspec);

        /* Store partition key information */
        StorePartitionKey(rel, stmt->partspec->strategy,
                          list_length(stmt->partspec->partParams),
                          /* ... other partition key details ... */);

        CommandCounterIncrement();
    }

    /* If creating a partition, inherit parent's indexes/triggers/FKs */
    if (stmt->partbound)
    {
        Oid parentId = linitial_oid(inheritOids);
        Relation parent = table_open(parentId, NoLock);
        List *idxlist = RelationGetIndexList(parent);

        /* Create indexes */
        foreach(listptr, idxlist)
        {
            /* ... create corresponding indexes ... */
        }

        /* Clone triggers and foreign keys */
        if (parent->trigdesc != NULL)
            CloneRowTriggersToPartition(parent, rel);

        CloneForeignKeyConstraints(NULL, parent, rel);

        table_close(parent, NoLock);
    }

    /* Add CHECK constraints */
    if (stmt->constraints)
        AddRelationNewConstraints(rel, NIL, stmt->constraints,
                                  true, true, false, queryString);

    ObjectAddressSet(address, RelationRelationId, relationId);

    /* Clean up */
    relation_close(rel, NoLock);

    return address;
}
```