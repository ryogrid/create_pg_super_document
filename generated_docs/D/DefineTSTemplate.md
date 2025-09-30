## Simplified Source

```c
ObjectAddress
DefineTSTemplate(List *names, List *parameters)
{
    char *tmplname;
    Oid tmplOid, namespaceoid;
    Relation tmplRel;
    HeapTuple tup;
    Datum values[Natts_pg_ts_template];
    bool nulls[Natts_pg_ts_template];
    ObjectAddress address;

    // Check superuser permissions
    if (!superuser())
        ereport(ERROR, "must be superuser to create text search templates");

    // Parse qualified name to get namespace and template name
    namespaceoid = QualifiedNameGetCreationNamespace(names, &tmplname);

    // Open catalog table and initialize values array
    tmplRel = table_open(TSTemplateRelationId, RowExclusiveLock);

    for (int i = 0; i < Natts_pg_ts_template; i++) {
        nulls[i] = false;
        values[i] = ObjectIdGetDatum(InvalidOid);
    }

    // Get new OID and set basic template info
    tmplOid = GetNewOidWithIndex(tmplRel, TSTemplateOidIndexId, Anum_pg_ts_dict_oid);
    values[Anum_pg_ts_template_oid - 1] = ObjectIdGetDatum(tmplOid);
    namestrcpy(&dname, tmplname);
    values[Anum_pg_ts_template_tmplname - 1] = NameGetDatum(&dname);
    values[Anum_pg_ts_template_tmplnamespace - 1] = ObjectIdGetDatum(namespaceoid);

    // Process parameters: init and lexize functions
    foreach(pl, parameters) {
        DefElem *defel = (DefElem *) lfirst(pl);

        if (strcmp(defel->defname, "init") == 0) {
            values[Anum_pg_ts_template_tmplinit - 1] =
                get_ts_template_func(defel, Anum_pg_ts_template_tmplinit);
        } else if (strcmp(defel->defname, "lexize") == 0) {
            values[Anum_pg_ts_template_tmpllexize - 1] =
                get_ts_template_func(defel, Anum_pg_ts_template_tmpllexize);
        } else {
            ereport(ERROR, "text search template parameter not recognized");
        }
    }

    // Validate required lexize function
    if (!OidIsValid(DatumGetObjectId(values[Anum_pg_ts_template_tmpllexize - 1])))
        ereport(ERROR, "text search template lexize method is required");

    // Insert into catalog
    tup = heap_form_tuple(tmplRel->rd_att, values, nulls);
    CatalogTupleInsert(tmplRel, tup);

    // Create dependencies and invoke hooks
    address = makeTSTemplateDependencies(tup);
    InvokeObjectPostCreateHook(TSTemplateRelationId, tmplOid, 0);

    // Cleanup
    heap_freetuple(tup);
    table_close(tmplRel, RowExclusiveLock);

    return address;
}
```