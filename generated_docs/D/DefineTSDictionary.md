# DefineTSDictionary

## Location
[src/backend/commands/tsearchcmds.c:393-487](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tsearchcmds.c#L393-L487)

## Overview
This function implements the CREATE TEXT SEARCH DICTIONARY SQL command, creating a new text search dictionary object in the system catalog with specified template and options.

## Definition
```c
ObjectAddress DefineTSDictionary(List *names, List *parameters)
```

## Simplified Source

```c
ObjectAddress
DefineTSDictionary(List *names, List *parameters)
{
    ListCell   *param_cell;
    Relation    dict_rel;
    HeapTuple   tuple;
    Datum       values[Natts_pg_ts_dict];
    bool        nulls[Natts_pg_ts_dict];
    NameData    dict_name_data;
    Oid         template_oid = InvalidOid;
    List       *dict_options = NIL;
    Oid         dict_oid;
    Oid         namespace_oid;
    char       *dict_name;
    ObjectAddress address;

    // Extract namespace and dictionary name from qualified name
    namespace_oid = QualifiedNameGetCreationNamespace(names, &dict_name);

    // Check creation permissions in target namespace
    AclResult acl_result = object_aclcheck(NamespaceRelationId, namespace_oid,
                                          GetUserId(), ACL_CREATE);
    if (acl_result != ACLCHECK_OK)
        aclcheck_error(acl_result, OBJECT_SCHEMA, get_namespace_name(namespace_oid));

    // Parse parameters to extract template and options
    foreach(param_cell, parameters)
    {
        DefElem *def_elem = (DefElem *) lfirst(param_cell);

        if (strcmp(def_elem->defname, "template") == 0)
        {
            template_oid = get_ts_template_oid(defGetQualifiedName(def_elem), false);
        }
        else
        {
            // Collect other parameters as dictionary options
            dict_options = lappend(dict_options, def_elem);
        }
    }

    // Validate that template was specified
    if (!OidIsValid(template_oid))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                 errmsg("text search template is required")));

    // Verify dictionary options are valid for this template
    verify_dictoptions(template_oid, dict_options);

    // Open dictionary catalog for insertion
    dict_rel = table_open(TSDictionaryRelationId, RowExclusiveLock);

    // Prepare tuple data
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    // Set dictionary attributes
    dict_oid = GetNewOidWithIndex(dict_rel, TSDictionaryOidIndexId, Anum_pg_ts_dict_oid);
    values[Anum_pg_ts_dict_oid - 1] = ObjectIdGetDatum(dict_oid);
    namestrcpy(&dict_name_data, dict_name);
    values[Anum_pg_ts_dict_dictname - 1] = NameGetDatum(&dict_name_data);
    values[Anum_pg_ts_dict_dictnamespace - 1] = ObjectIdGetDatum(namespace_oid);
    values[Anum_pg_ts_dict_dictowner - 1] = ObjectIdGetDatum(GetUserId());
    values[Anum_pg_ts_dict_dicttemplate - 1] = ObjectIdGetDatum(template_oid);

    // Serialize dictionary options if provided
    if (dict_options)
        values[Anum_pg_ts_dict_dictinitoption - 1] =
            PointerGetDatum(serialize_deflist(dict_options));
    else
        nulls[Anum_pg_ts_dict_dictinitoption - 1] = true;

    // Create and insert catalog tuple
    tuple = heap_form_tuple(dict_rel->rd_att, values, nulls);
    CatalogTupleInsert(dict_rel, tuple);

    // Establish dependencies and trigger hooks
    address = makeDictionaryDependencies(tuple);
    InvokeObjectPostCreateHook(TSDictionaryRelationId, dict_oid, 0);

    // Cleanup
    heap_freetuple(tuple);
    table_close(dict_rel, RowExclusiveLock);

    return address;
}
```