# getProcLangs

## Location
[src/bin/pg_dump/pg_dump.c:8508-8597](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L8508-L8597)

## Overview
Retrieves basic information about every procedural language in the PostgreSQL system for use by pg_dump during database backup operations.

## Definition

```c
ProcLangInfo *
getProcLangs(Archive *fout, int *numProcLangs)
```
## Detailed Description
The  function queries the  system catalog to retrieve information about all procedural languages that have the  flag set to true (indicating they are procedural languages rather than built-in languages). This function is part of the pg_dump utility's schema dumping process and must be called after  because it assumes that  functionality is available.

The function constructs a SQL query to fetch language metadata including permissions (ACLs), ownership, trusted status, and associated function OIDs. For each language found, it creates a  structure containing all relevant information needed for dumping the language definition.

## Parameters / Member Variables
- `*fout`: Archive pointer for the pg_dump operation, used for executing SQL queries
- `*numProcLangs`: Output parameter that receives the number of procedural languages found
## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [ProcLangInfo](../P/ProcLangInfo.md)
  - [pg_malloc](../p/pg_malloc.md)
  - atooid
  - [AssignDumpId](../A/AssignDumpId.md)
  - [getRoleName](getRoleName.md)
  - [selectDumpableProcLang](../s/selectDumpableProcLang.md)
  - [PQgetisnull](../P/PQgetisnull.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- Must be executed after getFuncs() due to dependency on findFuncByOid() functionality
- Queries only procedural languages (lanispl = true), excluding built-in languages
- Results are ordered by OID for consistent output
- Each language's dumpability is determined by selectDumpableProcLang()
- ACL information is preserved for languages that have explicit permissions set
- Memory allocation is performed for the entire result set at once using pg_malloc

## Simplified Source

```c
ProcLangInfo *
getProcLangs(Archive *fout, int *numProcLangs)
{
    PGresult   *res;
    int         ntups;
    PQExpBuffer query = createPQExpBuffer();
    ProcLangInfo *planginfo;

    // Query for all procedural languages (excluding built-in languages)
    appendPQExpBufferStr(query, "SELECT tableoid, oid, "
                         "lanname, lanpltrusted, lanplcallfoid, "
                         "laninline, lanvalidator, "
                         "lanacl, "
                         "acldefault('l', lanowner) AS acldefault, "
                         "lanowner "
                         "FROM pg_language "
                         "WHERE lanispl "
                         "ORDER BY oid");

    // Execute query and get results
    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    *numProcLangs = ntups;

    // Allocate memory for language info array
    planginfo = (ProcLangInfo *) pg_malloc(ntups * sizeof(ProcLangInfo));

    // Get column indices for result processing
    int i_tableoid = PQfnumber(res, "tableoid");
    int i_oid = PQfnumber(res, "oid");
    int i_lanname = PQfnumber(res, "lanname");
    int i_lanpltrusted = PQfnumber(res, "lanpltrusted");
    int i_lanplcallfoid = PQfnumber(res, "lanplcallfoid");
    int i_laninline = PQfnumber(res, "laninline");
    int i_lanvalidator = PQfnumber(res, "lanvalidator");
    int i_lanacl = PQfnumber(res, "lanacl");
    int i_acldefault = PQfnumber(res, "acldefault");
    int i_lanowner = PQfnumber(res, "lanowner");

    // Process each procedural language result
    for (int i = 0; i < ntups; i++) {
        // Initialize dump object metadata
        planginfo[i].dobj.objType = DO_PROCLANG;
        planginfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        planginfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&planginfo[i].dobj);

        // Copy language properties
        planginfo[i].dobj.name = pg_strdup(PQgetvalue(res, i, i_lanname));
        planginfo[i].dacl.acl = pg_strdup(PQgetvalue(res, i, i_lanacl));
        planginfo[i].dacl.acldefault = pg_strdup(PQgetvalue(res, i, i_acldefault));
        planginfo[i].dacl.privtype = 0;
        planginfo[i].dacl.initprivs = NULL;

        // Language-specific attributes
        planginfo[i].lanpltrusted = *(PQgetvalue(res, i, i_lanpltrusted)) == 't';
        planginfo[i].lanplcallfoid = atooid(PQgetvalue(res, i, i_lanplcallfoid));
        planginfo[i].laninline = atooid(PQgetvalue(res, i, i_laninline));
        planginfo[i].lanvalidator = atooid(PQgetvalue(res, i, i_lanvalidator));
        planginfo[i].lanowner = getRoleName(PQgetvalue(res, i, i_lanowner));

        // Determine if this language should be dumped
        selectDumpableProcLang(&(planginfo[i]), fout);

        // Mark if language has ACL for dumping
        if (!PQgetisnull(res, i, i_lanacl))
            planginfo[i].dobj.components |= DUMP_COMPONENT_ACL;
    }

    // Cleanup and return results
    PQclear(res);
    destroyPQExpBuffer(query);
    return planginfo;
}
```