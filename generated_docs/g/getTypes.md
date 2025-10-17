# getTypes

## Location
[src/bin/pg_dump/pg_dump.c:5847-6017](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L5847-L6017)

## Overview
Reads all data types from the PostgreSQL system catalogs and returns them as an array of TypeInfo structures for pg_dump processing, including built-in, user-defined, and array types.

## Definition

```c
TypeInfo *
getTypes(Archive *fout, int *numTypes)
```
## Detailed Description
This function is a comprehensive data type collection component of pg_dump that queries the pg_type system catalog to retrieve information about all data types in the database. It handles various type categories including built-in types, user-defined types, domains, composite types, enums, ranges, multiranges, and arrays.

Key operations performed:
1. Executes a complex SQL query joining pg_type with pg_class to get complete type metadata
2. Detects array types using element type relationships and naming patterns
3. Creates TypeInfo structures with proper dump object initialization and namespace resolution
4. Handles ACL information and determines dumpability based on dump options
5. Special processing for domain types to fetch constraint information
6. Creates shell type objects for base and range types that need I/O function definitions
7. Manages type dependencies and dump ordering requirements

The function must run after getFuncs() because it relies on function lookup capabilities for type dependencies.

## Parameters / Member Variables
- `*fout`: Archive structure containing dump configuration and output methods
- `*numTypes`: Output parameter that receives the total number of types found
## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - atooid
  - [AssignDumpId](../A/AssignDumpId.md)
  - [findNamespace](../f/findNamespace.md)
  - [getRoleName](getRoleName.md)
  - [selectDumpableType](../s/selectDumpableType.md)
  - [getDomainConstraints](getDomainConstraints.md)
  - [pg_malloc](../p/pg_malloc.md)
  - [pg_strdup](../p/pg_strdup.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- Must run after getFuncs() due to function dependency requirements
- Includes built-in types as they may be used as array elements by user-defined types
- Detects auto-generated array types reliably using element type's typarray field
- Creates shell type objects for base and range types needing I/O functions
- Handles domain constraints via getDomainConstraints for domain types
- Sets DUMP_COMPONENT_ACL flag for types with ACL information
- Uses complex SQL to determine array types and relation kinds
- Memory allocation uses pg_malloc for both TypeInfo and ShellTypeInfo arrays
- Returns allocated array that must be freed by caller
- Essential for complete type system representation during database dumps

## Simplified Source

```c
TypeInfo *getTypes(Archive *fout, int *numTypes)
{
    PGresult *res;
    int ntups, i;
    PQExpBuffer query = createPQExpBuffer();
    TypeInfo *tyinfo;
    ShellTypeInfo *stinfo;
    int i_tableoid, i_oid, i_typname, i_typnamespace, i_typacl, i_acldefault,
        i_typowner, i_typelem, i_typrelid, i_typrelkind, i_typtype,
        i_typisdefined, i_isarray;

    // Query all types including built-in ones for array element usage
    appendPQExpBufferStr(query,
                         "SELECT tableoid, oid, typname, typnamespace, typacl, "
                         "acldefault('T', typowner) AS acldefault, typowner, "
                         "typelem, typrelid, "
                         "CASE WHEN typrelid = 0 THEN ' '::\"char\" "
                         "ELSE (SELECT relkind FROM pg_class WHERE oid = typrelid) END AS typrelkind, "
                         "typtype, typisdefined, "
                         "typname[0] = '_' AND typelem != 0 AND "
                         "(SELECT typarray FROM pg_type te WHERE oid = pg_type.typelem) = oid AS isarray "
                         "FROM pg_type");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);

    // Allocate array for type info
    tyinfo = (TypeInfo *) pg_malloc(ntups * sizeof(TypeInfo));

    // Get column indices
    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_typname = PQfnumber(res, "typname");
    i_typnamespace = PQfnumber(res, "typnamespace");
    i_typacl = PQfnumber(res, "typacl");
    i_acldefault = PQfnumber(res, "acldefault");
    i_typowner = PQfnumber(res, "typowner");
    i_typelem = PQfnumber(res, "typelem");
    i_typrelid = PQfnumber(res, "typrelid");
    i_typrelkind = PQfnumber(res, "typrelkind");
    i_typtype = PQfnumber(res, "typtype");
    i_typisdefined = PQfnumber(res, "typisdefined");
    i_isarray = PQfnumber(res, "isarray");

    // Process each type
    for (i = 0; i < ntups; i++) {
        // Initialize dump object
        tyinfo[i].dobj.objType = DO_TYPE;
        tyinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        tyinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&tyinfo[i].dobj);
        tyinfo[i].dobj.name = pg_strdup(PQgetvalue(res, i, i_typname));
        tyinfo[i].dobj.namespace = findNamespace(atooid(PQgetvalue(res, i, i_typnamespace)));

        // Set ACL information
        tyinfo[i].dacl.acl = pg_strdup(PQgetvalue(res, i, i_typacl));
        tyinfo[i].dacl.acldefault = pg_strdup(PQgetvalue(res, i, i_acldefault));
        tyinfo[i].dacl.privtype = 0;
        tyinfo[i].dacl.initprivs = NULL;
        tyinfo[i].ftypname = NULL;

        // Set type properties
        tyinfo[i].rolname = getRoleName(PQgetvalue(res, i, i_typowner));
        tyinfo[i].typelem = atooid(PQgetvalue(res, i, i_typelem));
        tyinfo[i].typrelid = atooid(PQgetvalue(res, i, i_typrelid));
        tyinfo[i].typrelkind = *PQgetvalue(res, i, i_typrelkind);
        tyinfo[i].typtype = *PQgetvalue(res, i, i_typtype);
        tyinfo[i].shellType = NULL;

        // Set boolean flags
        tyinfo[i].isDefined = (strcmp(PQgetvalue(res, i, i_typisdefined), "t") == 0);
        tyinfo[i].isArray = (strcmp(PQgetvalue(res, i, i_isarray), "t") == 0);
        tyinfo[i].isMultirange = (tyinfo[i].typtype == TYPTYPE_MULTIRANGE);

        // Determine if this type should be dumped
        selectDumpableType(&tyinfo[i], fout);

        // Mark ACL component if present
        if (!PQgetisnull(res, i, i_typacl))
            tyinfo[i].dobj.components |= DUMP_COMPONENT_ACL;

        // Handle domain constraints
        tyinfo[i].nDomChecks = 0;
        tyinfo[i].domChecks = NULL;
        tyinfo[i].notnull = NULL;
        if ((tyinfo[i].dobj.dump & DUMP_COMPONENT_DEFINITION) &&
            tyinfo[i].typtype == TYPTYPE_DOMAIN)
            getDomainConstraints(fout, &(tyinfo[i]));

        // Create shell type for base and range types
        if ((tyinfo[i].dobj.dump & DUMP_COMPONENT_DEFINITION) &&
            (tyinfo[i].typtype == TYPTYPE_BASE || tyinfo[i].typtype == TYPTYPE_RANGE)) {
            stinfo = (ShellTypeInfo *) pg_malloc(sizeof(ShellTypeInfo));
            stinfo->dobj.objType = DO_SHELL_TYPE;
            stinfo->dobj.catId = nilCatalogId;
            AssignDumpId(&stinfo->dobj);
            stinfo->dobj.name = pg_strdup(tyinfo[i].dobj.name);
            stinfo->dobj.namespace = tyinfo[i].dobj.namespace;
            stinfo->baseType = &(tyinfo[i]);
            tyinfo[i].shellType = stinfo;

            // Initially don't dump shell type - will be decided during dependency sorting
            stinfo->dobj.dump = DUMP_COMPONENT_NONE;
        }
    }

    *numTypes = ntups;
    PQclear(res);
    destroyPQExpBuffer(query);

    return tyinfo;
}
```