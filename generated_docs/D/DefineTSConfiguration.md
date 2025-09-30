# DefineTSConfiguration

## Location
[src/backend/commands/tsearchcmds.c:899-1107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tsearchcmds.c#L899-L1107)

## Overview
Creates a new text search configuration in the system catalog, optionally copying token-dictionary mappings from an existing configuration.

## Definition
```c
ObjectAddress DefineTSConfiguration(List *names, List *parameters, ObjectAddress *copied)
```

## Detailed Description
DefineTSConfiguration implements the CREATE TEXT SEARCH CONFIGURATION SQL command. It validates parameters, checks permissions, creates the configuration tuple in pg_ts_config, and optionally copies token-dictionary mappings from a source configuration. The function supports two modes: creating a configuration with a specified parser, or copying an existing configuration. It uses batch insertion for efficiency when copying large configuration maps and establishes all necessary dependency relationships.

## Parameters / Member Variables
- `names`: List containing the qualified or unqualified name components for the new configuration
- `parameters`: List of DefElem nodes containing configuration options ("parser" or "copy")
- `copied`: Output parameter set to the ObjectAddress of the copied configuration, or NULL if not copying

## Dependencies
- Functions called/Symbols referenced:
  - [QualifiedNameGetCreationNamespace](../Q/QualifiedNameGetCreationNamespace.md) (resolves namespace and name)
  - [object_aclcheck](../o/object_aclcheck.md) (checks CREATE permission on namespace)
  - [get_ts_parser_oid](../g/get_ts_parser_oid.md) (resolves parser name to OID)
  - [get_ts_config_oid](../g/get_ts_config_oid.md) (resolves source config name to OID)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md) (allocates new OID for configuration)
  - [heap_form_tuple](../h/heap_form_tuple.md)/CatalogTupleInsert (creates configuration tuple)
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)/ExecDropSingleTupleTableSlot (manages tuple slots)
  - [CatalogTuplesMultiInsertWithInfo](../C/CatalogTuplesMultiInsertWithInfo.md) (batch inserts map entries)
  - [makeConfigurationDependencies](../m/makeConfigurationDependencies.md) (establishes dependencies)
  - InvokeObjectPostCreateHook (triggers post-creation hooks)
  - [heap_freetuple](../h/heap_freetuple.md) (cleanup)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (SQL command processing)

## Notes and Other Information
- Validates mutually exclusive PARSER and COPY options
- Requires CREATE privilege on target namespace
- Uses batch insertion with configurable slot count for map copying efficiency
- Copies all token-dictionary mappings when using COPY option
- Returns ObjectAddress for use in dependency tracking
- Supports extension membership through makeConfigurationDependencies
- Uses RowExclusiveLock on both pg_ts_config and pg_ts_config_map relations
- Implements proper error handling for missing parsers and configurations

## Simplified Source

```c
ObjectAddress DefineTSConfiguration(List *names, List *parameters, ObjectAddress *copied) {
    Relation cfgRel, mapRel = NULL;
    HeapTuple tup;
    Datum values[Natts_pg_ts_config];
    bool nulls[Natts_pg_ts_config];
    Oid namespaceoid, sourceOid = InvalidOid, prsOid = InvalidOid, cfgOid;
    char *cfgname;
    NameData cname;
    ObjectAddress address;

    // Parse qualified name and get namespace
    namespaceoid = QualifiedNameGetCreationNamespace(names, &cfgname);

    // Check creation permissions
    aclresult = object_aclcheck(NamespaceRelationId, namespaceoid, GetUserId(), ACL_CREATE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(namespaceoid));

    // Parse configuration parameters
    foreach(pl, parameters) {
        DefElem *defel = (DefElem *) lfirst(pl);

        if (strcmp(defel->defname, "parser") == 0)
            prsOid = get_ts_parser_oid(defGetQualifiedName(defel), false);
        else if (strcmp(defel->defname, "copy") == 0)
            sourceOid = get_ts_config_oid(defGetQualifiedName(defel), false);
        else
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                           errmsg("text search configuration parameter \"%s\" not recognized",
                                  defel->defname)));
    }

    // Validate mutually exclusive options
    if (OidIsValid(sourceOid) && OidIsValid(prsOid))
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                       errmsg("cannot specify both PARSER and COPY options")));

    // Return copied configuration info if requested
    if (copied && OidIsValid(sourceOid))
        ObjectAddressSet(*copied, TSConfigRelationId, sourceOid);

    // Get parser from source configuration if copying
    if (OidIsValid(sourceOid)) {
        tup = SearchSysCache1(TSCONFIGOID, ObjectIdGetDatum(sourceOid));
        if (!HeapTupleIsValid(tup))
            elog(ERROR, "cache lookup failed for text search configuration %u", sourceOid);

        Form_pg_ts_config cfg = (Form_pg_ts_config) GETSTRUCT(tup);
        prsOid = cfg->cfgparser;
        ReleaseSysCache(tup);
    }

    // Validate parser is specified
    if (!OidIsValid(prsOid))
        ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                       errmsg("text search parser is required")));

    // Create configuration entry
    cfgRel = table_open(TSConfigRelationId, RowExclusiveLock);

    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    cfgOid = GetNewOidWithIndex(cfgRel, TSConfigOidIndexId, Anum_pg_ts_config_oid);
    values[Anum_pg_ts_config_oid - 1] = ObjectIdGetDatum(cfgOid);
    namestrcpy(&cname, cfgname);
    values[Anum_pg_ts_config_cfgname - 1] = NameGetDatum(&cname);
    values[Anum_pg_ts_config_cfgnamespace - 1] = ObjectIdGetDatum(namespaceoid);
    values[Anum_pg_ts_config_cfgowner - 1] = ObjectIdGetDatum(GetUserId());
    values[Anum_pg_ts_config_cfgparser - 1] = ObjectIdGetDatum(prsOid);

    tup = heap_form_tuple(cfgRel->rd_att, values, nulls);
    CatalogTupleInsert(cfgRel, tup);

    // Copy token-dictionary mappings if source specified
    if (OidIsValid(sourceOid)) {
        ScanKeyData skey;
        SysScanDesc scan;
        HeapTuple maptup;
        TupleTableSlot **slot;
        CatalogIndexState indstate;
        int max_slots, slot_init_count = 0, slot_stored_count = 0;

        mapRel = table_open(TSConfigMapRelationId, RowExclusiveLock);
        indstate = CatalogOpenIndexes(mapRel);

        // Allocate slots for batch insertion
        max_slots = MAX_CATALOG_MULTI_INSERT_BYTES / sizeof(FormData_pg_ts_config_map);
        slot = palloc(sizeof(TupleTableSlot *) * max_slots);

        ScanKeyInit(&skey, Anum_pg_ts_config_map_mapcfg, BTEqualStrategyNumber,
                   F_OIDEQ, ObjectIdGetDatum(sourceOid));

        scan = systable_beginscan(mapRel, TSConfigMapIndexId, true, NULL, 1, &skey);

        // Copy each mapping entry
        while (HeapTupleIsValid((maptup = systable_getnext(scan)))) {
            Form_pg_ts_config_map cfgmap = (Form_pg_ts_config_map) GETSTRUCT(maptup);

            // Initialize slot if needed
            if (slot_init_count < max_slots) {
                slot[slot_stored_count] = MakeSingleTupleTableSlot(RelationGetDescr(mapRel),
                                                                  &TTSOpsHeapTuple);
                slot_init_count++;
            }

            ExecClearTuple(slot[slot_stored_count]);
            memset(slot[slot_stored_count]->tts_isnull, false,
                   slot[slot_stored_count]->tts_tupleDescriptor->natts * sizeof(bool));

            // Copy mapping data with new configuration OID
            slot[slot_stored_count]->tts_values[Anum_pg_ts_config_map_mapcfg - 1] = cfgOid;
            slot[slot_stored_count]->tts_values[Anum_pg_ts_config_map_maptokentype - 1] = cfgmap->maptokentype;
            slot[slot_stored_count]->tts_values[Anum_pg_ts_config_map_mapseqno - 1] = cfgmap->mapseqno;
            slot[slot_stored_count]->tts_values[Anum_pg_ts_config_map_mapdict - 1] = cfgmap->mapdict;

            ExecStoreVirtualTuple(slot[slot_stored_count]);
            slot_stored_count++;

            // Batch insert when slots are full
            if (slot_stored_count == max_slots) {
                CatalogTuplesMultiInsertWithInfo(mapRel, slot, slot_stored_count, indstate);
                slot_stored_count = 0;
            }
        }

        // Insert remaining tuples
        if (slot_stored_count > 0)
            CatalogTuplesMultiInsertWithInfo(mapRel, slot, slot_stored_count, indstate);

        // Cleanup
        for (int i = 0; i < slot_init_count; i++)
            ExecDropSingleTupleTableSlot(slot[i]);

        systable_endscan(scan);
        CatalogCloseIndexes(indstate);
    }

    // Create dependencies and invoke hooks
    address = makeConfigurationDependencies(tup, false, mapRel);
    InvokeObjectPostCreateHook(TSConfigRelationId, cfgOid, 0);

    heap_freetuple(tup);
    if (mapRel)
        table_close(mapRel, RowExclusiveLock);
    table_close(cfgRel, RowExclusiveLock);

    return address;
}
```