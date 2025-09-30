## Simplified Source

```c
static ObjectAddress
makeTSTemplateDependencies(HeapTuple tuple)
{
    Form_pg_ts_template tmpl = (Form_pg_ts_template) GETSTRUCT(tuple);
    ObjectAddress myself, referenced;
    ObjectAddresses *addrs;

    // Set up template object address
    ObjectAddressSet(myself, TSTemplateRelationId, tmpl->oid);

    // Record dependency on current extension
    recordDependencyOnCurrentExtension(&myself, false);

    addrs = new_object_addresses();

    // Add dependency on namespace
    ObjectAddressSet(referenced, NamespaceRelationId, tmpl->tmplnamespace);
    add_exact_object_address(&referenced, addrs);

    // Add dependency on required lexize function
    ObjectAddressSet(referenced, ProcedureRelationId, tmpl->tmpllexize);
    add_exact_object_address(&referenced, addrs);

    // Add dependency on optional init function if present
    if (OidIsValid(tmpl->tmplinit)) {
        referenced.objectId = tmpl->tmplinit;
        add_exact_object_address(&referenced, addrs);
    }

    // Record all dependencies and cleanup
    record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL);
    free_object_addresses(addrs);

    return myself;
}
```