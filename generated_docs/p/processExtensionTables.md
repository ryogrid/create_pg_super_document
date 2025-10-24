# processExtensionTables

## Location
[src/bin/pg_dump/pg_dump.c:18364-18544](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L18364-L18544)

## Overview
Handles extension configuration tables by identifying them for data dumping and managing foreign key dependencies between configuration tables to ensure proper restoration order.

## Definition
```c
void processExtensionTables(Archive *fout, ExtensionInfo extinfo[], int numExtensions)
```

## Detailed Description
This function performs two critical tasks for extension configuration tables:

1. **Identifies and creates dump records for extension configuration tables**: Extension configuration tables are user-modifiable tables whose structure is managed by CREATE EXTENSION but whose data needs to be preserved during dumps. The function creates TableDataInfo objects for these tables to ensure their data is dumped even when the table structure itself is not.

2. **Records foreign key dependencies between configuration tables**: Since foreign keys are created at CREATE EXTENSION time (before data loading), the function determines the optimal restoration order to avoid FK violations. It queries pg_constraint to find FK relationships and registers dependencies between TableDataInfo objects.

The function handles extension include/exclude lists, table-specific include/exclude lists, and schema-level exclusions. It also processes extension condition arrays that can filter which rows are dumped from configuration tables.

## Parameters / Member Variables
- `fout`: Archive context for the dump operation containing dump options
- `extinfo[]`: Array of ExtensionInfo structures containing extension metadata including configuration tables
- `numExtensions`: Number of extensions in the extinfo array

## Dependencies
- Functions called/Symbols referenced:
  - [simple_oid_list_member](../s/simple_oid_list_member.md)
  - [parsePGArray](parsePGArray.md)
  - atooid
  - [findTableByOid](../f/findTableByOid.md)
  - [makeTableDataInfo](../m/makeTableDataInfo.md)
  - [pg_strdup](pg_strdup.md)
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [printfPQExpBuffer](printfPQExpBuffer.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQfnumber](../P/PQfnumber.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [addObjectDependency](../a/addObjectDependency.md)
  - [PQclear](../P/PQclear.md)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md)
  - [pg_fatal](pg_fatal.md)
- Called from:
  - [getSchemaData](../g/getSchemaData.md) (in src/bin/pg_dump/common.c:223)

## Notes and Other Information
- Configuration table data is treated as schema data, so TableDataInfo objects are created even in schema-only mode
- The function cannot handle circular FK dependencies and will produce invalid dumps in such cases (documented limitation)
- Extension configuration and condition arrays must have matching lengths
- FK dependency management ensures data can be restored without constraint violations
- Supports complex filtering via extension include/exclude lists and table/schema-specific exclusions
- Extension condition strings can be used to filter specific rows from configuration tables during dump

## Simplified Source

```c
void
processExtensionTables(Archive *fout, ExtensionInfo extinfo[], int numExtensions)
{
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer query;
    PGresult *res;
    int ntups, i;
    int i_conrelid, i_confrelid;

    // Early return if no extensions
    if (numExtensions == 0)
        return;

    // Phase 1: Create TableDataInfo objects for extension configuration tables
    for (i = 0; i < numExtensions; i++) {
        ExtensionInfo *curext = &(extinfo[i]);
        char *extconfig = curext->extconfig;
        char *extcondition = curext->extcondition;
        char **extconfigarray = NULL;
        char **extconditionarray = NULL;
        int nconfigitems = 0;
        int nconditionitems = 0;

        // Check extension include/exclude lists
        if (extension_include_oids.head != NULL &&
            !simple_oid_list_member(&extension_include_oids, curext->dobj.catId.oid))
            continue;

        if (extension_exclude_oids.head != NULL &&
            simple_oid_list_member(&extension_exclude_oids, curext->dobj.catId.oid))
            continue;

        // Process configuration tables if present
        if (strlen(extconfig) != 0 || strlen(extcondition) != 0) {
            int j;

            // Parse configuration and condition arrays
            if (!parsePGArray(extconfig, &extconfigarray, &nconfigitems))
                pg_fatal("could not parse %s array", "extconfig");
            if (!parsePGArray(extcondition, &extconditionarray, &nconditionitems))
                pg_fatal("could not parse %s array", "extcondition");
            if (nconfigitems != nconditionitems)
                pg_fatal("mismatched number of configurations and conditions for extension");

            // Process each configuration table
            for (j = 0; j < nconfigitems; j++) {
                TableInfo *configtbl;
                Oid configtbloid = atooid(extconfigarray[j]);
                bool dumpobj = curext->dobj.dump & DUMP_COMPONENT_DEFINITION;

                configtbl = findTableByOid(configtbloid);
                if (configtbl == NULL)
                    continue;

                // Apply various include/exclude filters
                if (!(curext->dobj.dump & DUMP_COMPONENT_DEFINITION)) {
                    // Check table explicitly requested
                    if (table_include_oids.head != NULL &&
                        simple_oid_list_member(&table_include_oids, configtbloid))
                        dumpobj = true;

                    // Check table's schema explicitly requested
                    if (configtbl->dobj.namespace->dobj.dump & DUMP_COMPONENT_DATA)
                        dumpobj = true;
                }

                // Check exclusions
                if (table_exclude_oids.head != NULL &&
                    simple_oid_list_member(&table_exclude_oids, configtbloid))
                    dumpobj = false;

                if (simple_oid_list_member(&schema_exclude_oids,
                                          configtbl->dobj.namespace->dobj.catId.oid))
                    dumpobj = false;

                // Create TableDataInfo if approved for dumping
                if (dumpobj) {
                    makeTableDataInfo(dopt, configtbl);
                    if (configtbl->dataObj != NULL) {
                        if (strlen(extconditionarray[j]) > 0)
                            configtbl->dataObj->filtercond = pg_strdup(extconditionarray[j]);
                    }
                }
            }
        }

        if (extconfigarray)
            free(extconfigarray);
        if (extconditionarray)
            free(extconditionarray);
    }

    // Phase 2: Register FK dependencies between configuration tables
    query = createPQExpBuffer();

    printfPQExpBuffer(query,
                     "SELECT conrelid, confrelid "
                     "FROM pg_constraint "
                     "JOIN pg_depend ON (objid = confrelid) "
                     "WHERE contype = 'f' "
                     "AND refclassid = 'pg_extension'::regclass "
                     "AND classid = 'pg_class'::regclass;");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);

    i_conrelid = PQfnumber(res, "conrelid");
    i_confrelid = PQfnumber(res, "confrelid");

    // Register FK dependencies
    for (i = 0; i < ntups; i++) {
        Oid conrelid, confrelid;
        TableInfo *reftable, *contable;

        conrelid = atooid(PQgetvalue(res, i, i_conrelid));
        confrelid = atooid(PQgetvalue(res, i, i_confrelid));
        contable = findTableByOid(conrelid);
        reftable = findTableByOid(confrelid);

        if (reftable == NULL || reftable->dataObj == NULL ||
            contable == NULL || contable->dataObj == NULL)
            continue;

        // Make referencing table depend on referenced table's data
        addObjectDependency(&contable->dataObj->dobj, reftable->dataObj->dobj.dumpId);
    }

    PQclear(res);
    destroyPQExpBuffer(query);
}
```