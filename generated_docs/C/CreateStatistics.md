# CreateStatistics

## Location
[src/backend/commands/statscmds.c:62-598](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/statscmds.c#L62-L598)

## Overview
Creates a new PostgreSQL extended statistics object that tracks column correlations, functional dependencies, and other multivariate statistics to improve query planner estimates.

## Definition

```c
struct the char array of enabled statistic types */
	ntypes = 0;
```
## Detailed Description
This function implements the CREATE STATISTICS SQL command, which creates extended statistics objects on table columns and expressions. Extended statistics help the query planner make better estimates for queries involving multiple correlated columns by tracking relationships like functional dependencies, n-distinct counts, and most common value lists across column combinations.

The function performs extensive validation including checking relation permissions, column existence, data type compatibility, and duplicate detection. It supports statistics on regular columns, expressions, or a combination of both. The created statistics object is stored in the pg_statistic_ext system catalog with appropriate dependency tracking.

Key features include:
- Support for multiple statistics types (ndistinct, dependencies, mcv, expressions)
- Validation of column references and expressions  
- Automatic name generation when not specified
- Comprehensive dependency tracking for columns, expressions, namespace, and owner
- Integration with PostgreSQL's object management system

## Parameters / Member Variables
- : CreateStatsStmt structure containing the parsed CREATE STATISTICS command with relation names, column/expression lists, statistics types, and optional name/comment

## Dependencies
- Functions called/Symbols referenced:
  - [relation_openrv](../r/relation_openrv.md) (opens the target relation)
  - [ChooseExtendedStatisticName](ChooseExtendedStatisticName.md) (generates automatic names)
  - [SearchSysCacheAttName](../S/SearchSysCacheAttName.md) (validates column references)
  - [compare_int16](../c/compare_int16.md) (sorts column attribute numbers)
  - [buildint2vector](../b/buildint2vector.md) (creates column number array)
  - [CatalogTupleInsert](CatalogTupleInsert.md) (inserts into pg_statistic_ext)
  - [recordDependencyOn](../r/recordDependencyOn.md) (tracks object dependencies)
  - [CreateComments](CreateComments.md) (adds optional comments)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (src/backend/tcop/utility.c:1902)
  - [ATExecAddStatistics](../A/ATExecAddStatistics.md) (src/backend/commands/tablecmds.c:9252)

## Notes and Other Information
- Supports statistics on regular tables, materialized views, foreign tables, and partitioned tables
- Requires ShareUpdateExclusiveLock on the target relation to avoid conflicts with concurrent ANALYZE
- Maximum of STATS_MAX_DIMENSIONS (32) columns allowed per statistics object
- System columns cannot be included in extended statistics
- Data types must have a default btree operator class (less-than operator)
- Creates automatic dependencies so statistics are dropped when referenced columns are dropped
- Statistics objects are not considered extension members (no ALTER EXTENSION support)
- Returns InvalidObjectAddress if IF NOT EXISTS is specified and object already exists

## Simplified Source

```c
ObjectAddress CreateStatistics(CreateStatsStmt *stmt)
{
    int16 attnums[STATS_MAX_DIMENSIONS];
    int nattnums = 0;
    char *namestr;
    Oid statoid, namespaceId, relid;
    Relation rel = NULL;
    List *stxexprs = NIL;
    bool build_ndistinct = false, build_dependencies = false, build_mcv = false;
    bool build_expressions = false, requested_type = false;

    // Validate single relation requirement
    if (list_length(stmt->relations) != 1)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("only a single relation is allowed in CREATE STATISTICS")));

    // Open relation with appropriate lock
    RangeVar *rln = (RangeVar *) linitial(stmt->relations);
    rel = relation_openrv(rln, ShareUpdateExclusiveLock);

    // Validate relation type and ownership
    if (rel->rd_rel->relkind != RELKIND_RELATION &&
        rel->rd_rel->relkind != RELKIND_MATVIEW &&
        rel->rd_rel->relkind != RELKIND_FOREIGN_TABLE &&
        rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                       errmsg("cannot define statistics for relation \"%s\"",
                              RelationGetRelationName(rel))));

    if (!object_ownercheck(RelationRelationId, RelationGetRelid(rel), GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(rel->rd_rel->relkind),
                      RelationGetRelationName(rel));

    relid = RelationGetRelid(rel);

    // Determine namespace and name
    if (stmt->defnames)
        namespaceId = QualifiedNameGetCreationNamespace(stmt->defnames, &namestr);
    else {
        namespaceId = RelationGetNamespace(rel);
        namestr = ChooseExtendedStatisticName(RelationGetRelationName(rel),
                                            ChooseExtendedStatisticNameAddition(stmt->exprs),
                                            "stat", namespaceId);
    }

    // Check for duplicate statistics object
    if (SearchSysCacheExists2(STATEXTNAMENSP, CStringGetDatum(namestr),
                            ObjectIdGetDatum(namespaceId))) {
        if (stmt->if_not_exists) {
            relation_close(rel, NoLock);
            return InvalidObjectAddress;
        }
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                       errmsg("statistics object \"%s\" already exists", namestr)));
    }

    // Validate column/expression count
    int numcols = list_length(stmt->exprs);
    if (numcols > STATS_MAX_DIMENSIONS)
        ereport(ERROR, (errcode(ERRCODE_TOO_MANY_COLUMNS),
                       errmsg("cannot have more than %d columns in statistics",
                              STATS_MAX_DIMENSIONS)));

    // Process columns and expressions
    ListCell *cell;
    foreach(cell, stmt->exprs) {
        StatsElem *selem = lfirst_node(StatsElem, cell);

        if (selem->name) {  // Column reference
            HeapTuple atttuple = SearchSysCacheAttName(relid, selem->name);
            if (!HeapTupleIsValid(atttuple))
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                               errmsg("column \"%s\" does not exist", selem->name)));

            Form_pg_attribute attForm = (Form_pg_attribute) GETSTRUCT(atttuple);

            // Validate column constraints
            if (attForm->attnum <= 0)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                               errmsg("statistics creation on system columns is not supported")));

            attnums[nattnums++] = attForm->attnum;
            ReleaseSysCache(atttuple);
        }
        else {  // Expression
            stxexprs = lappend(stxexprs, selem->expr);
        }
    }

    // Parse statistics types
    foreach(cell, stmt->stat_types) {
        char *type = strVal(lfirst(cell));
        if (strcmp(type, "ndistinct") == 0) {
            build_ndistinct = true;
            requested_type = true;
        }
        else if (strcmp(type, "dependencies") == 0) {
            build_dependencies = true;
            requested_type = true;
        }
        else if (strcmp(type, "mcv") == 0) {
            build_mcv = true;
            requested_type = true;
        }
    }

    // Set defaults if no specific types requested
    if (!requested_type && numcols >= 2) {
        build_ndistinct = build_dependencies = build_mcv = true;
    }
    build_expressions = (stxexprs != NIL);

    // Validate minimum requirements
    if (numcols < 2 && list_length(stxexprs) != 1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                       errmsg("extended statistics require at least 2 columns")));

    // Sort and check for duplicates
    qsort(attnums, nattnums, sizeof(int16), compare_int16);
    for (int i = 1; i < nattnums; i++) {
        if (attnums[i] == attnums[i - 1])
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_COLUMN),
                           errmsg("duplicate column name in statistics definition")));
    }

    // Create catalog entry
    Relation statrel = table_open(StatisticExtRelationId, RowExclusiveLock);

    // Build tuple data
    Datum values[Natts_pg_statistic_ext];
    bool nulls[Natts_pg_statistic_ext];
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    statoid = GetNewOidWithIndex(statrel, StatisticExtOidIndexId,
                               Anum_pg_statistic_ext_oid);
    values[Anum_pg_statistic_ext_oid - 1] = ObjectIdGetDatum(statoid);
    values[Anum_pg_statistic_ext_stxrelid - 1] = ObjectIdGetDatum(relid);
    values[Anum_pg_statistic_ext_stxname - 1] = NameGetDatum(&(NameData){});
    namestrcpy((NameData*)DatumGetPointer(values[Anum_pg_statistic_ext_stxname - 1]), namestr);
    values[Anum_pg_statistic_ext_stxnamespace - 1] = ObjectIdGetDatum(namespaceId);
    values[Anum_pg_statistic_ext_stxowner - 1] = ObjectIdGetDatum(GetUserId());

    // Build enabled statistics types array
    Datum types[4];
    int ntypes = 0;
    if (build_ndistinct) types[ntypes++] = CharGetDatum(STATS_EXT_NDISTINCT);
    if (build_dependencies) types[ntypes++] = CharGetDatum(STATS_EXT_DEPENDENCIES);
    if (build_mcv) types[ntypes++] = CharGetDatum(STATS_EXT_MCV);
    if (build_expressions) types[ntypes++] = CharGetDatum(STATS_EXT_EXPRESSIONS);

    values[Anum_pg_statistic_ext_stxkeys - 1] = PointerGetDatum(buildint2vector(attnums, nattnums));
    values[Anum_pg_statistic_ext_stxkind - 1] = PointerGetDatum(construct_array_builtin(types, ntypes, CHAROID));

    // Handle expressions
    if (stxexprs != NIL) {
        char *exprsString = nodeToString(stxexprs);
        values[Anum_pg_statistic_ext_stxexprs - 1] = CStringGetTextDatum(exprsString);
    } else {
        nulls[Anum_pg_statistic_ext_stxexprs - 1] = true;
    }

    // Insert tuple and record dependencies
    HeapTuple htup = heap_form_tuple(statrel->rd_att, values, nulls);
    CatalogTupleInsert(statrel, htup);
    heap_freetuple(htup);
    relation_close(statrel, RowExclusiveLock);

    // Record dependencies
    ObjectAddress myself;
    ObjectAddressSet(myself, StatisticExtRelationId, statoid);

    // Dependencies on columns
    for (int i = 0; i < nattnums; i++) {
        ObjectAddress parentobject;
        ObjectAddressSubSet(parentobject, RelationRelationId, relid, attnums[i]);
        recordDependencyOn(&myself, &parentobject, DEPENDENCY_AUTO);
    }

    // Dependencies on expressions, namespace, and owner
    if (stxexprs)
        recordDependencyOnSingleRelExpr(&myself, (Node *) stxexprs, relid,
                                      DEPENDENCY_NORMAL, DEPENDENCY_AUTO, false);

    ObjectAddress parentobject;
    ObjectAddressSet(parentobject, NamespaceRelationId, namespaceId);
    recordDependencyOn(&myself, &parentobject, DEPENDENCY_NORMAL);
    recordDependencyOnOwner(StatisticExtRelationId, statoid, GetUserId());

    // Add comment if specified
    if (stmt->stxcomment != NULL)
        CreateComments(statoid, StatisticExtRelationId, 0, stmt->stxcomment);

    CacheInvalidateRelcache(rel);
    relation_close(rel, NoLock);
    InvokeObjectPostCreateHook(StatisticExtRelationId, statoid, 0);

    return myself;
}
```