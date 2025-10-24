# dumpFunc

## Location
[src/bin/pg_dump/pg_dump.c:12312-12727](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L12312-L12727)

## Overview
Generates SQL DDL statements to recreate a PostgreSQL function, including all its attributes, parameters, and metadata during a database dump operation.

## Definition
```c
static void dumpFunc(Archive *fout, const FuncInfo *finfo)
```

## Detailed Description
This function is responsible for dumping a complete SQL CREATE FUNCTION (or PROCEDURE) statement to recreate a PostgreSQL function. It handles all function attributes including volatility, strictness, security properties, cost settings, parallelism, language, transforms, and configuration parameters. The function uses prepared statements for efficiency when dumping multiple functions and adapts its behavior based on the PostgreSQL server version to ensure compatibility across different releases.

Key responsibilities include:
- Building comprehensive CREATE FUNCTION/PROCEDURE statements
- Handling different function types (regular functions, procedures, window functions)
- Managing function source code, binary paths, and SQL body formats
- Processing function configuration parameters and GUC settings
- Generating appropriate DROP statements for clean replacements
- Adding comments, security labels, and ACL information
- Supporting binary upgrade scenarios

## Parameters / Member Variables
- `fout`: Archive structure containing dump context, options, and output formatting information
- `finfo`: FuncInfo structure containing complete function metadata including OID, name, arguments, return type, and various function attributes

## Dependencies
- Functions called/Symbols referenced:
  - [format_function_arguments](../f/format_function_arguments.md)
  - [format_function_signature](../f/format_function_signature.md)
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)
  - appendStringLiteralAH
  - [appendStringLiteralDQ](../a/appendStringLiteralDQ.md)
  - [parsePGArray](../p/parsePGArray.md)
  - [parseOidArray](../p/parseOidArray.md)
  - [getFormattedTypeName](../g/getFormattedTypeName.md)
  - [variable_is_guc_list_quote](../v/variable_is_guc_list_quote.md)
  - [SplitGUCList](../S/SplitGUCList.md)
  - [append_depends_on_extension](../a/append_depends_on_extension.md)
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [dumpSecLabel](dumpSecLabel.md)
  - [dumpACL](dumpACL.md)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)
  - fmtQualifiedDumpable

## Notes and Other Information
- The function is skipped entirely during data-only dumps (when dopt->dataOnly is true)
- Uses prepared statements (PREPQUERY_DUMPFUNC) for performance optimization when dumping multiple functions
- Adapts SQL generation based on PostgreSQL version (supports features from 9.5+ to 14.0+)
- Handles three different function source formats: SQL body (14.0+), binary + source, or source only
- Processes GUC configuration parameters with special handling for list-quote variables
- Supports both functions and procedures (introduced in PostgreSQL 11)
- Includes comprehensive error handling for invalid function attributes
- Memory management uses PostgreSQL's PQExpBuffer system with proper cleanup
- Function signatures are generated in multiple formats for different purposes (identity vs full signatures)

## Simplified Source

```c
static void
dumpFunc(Archive *fout, const FuncInfo *finfo)
{
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer query, q, delqry, asPart;
    PGresult *res;
    char *funcsig, *funcfullsig = NULL, *funcsig_tag, *qual_funcsig;
    char *proretset, *prosrc, *probin, *prosqlbody;
    char *funcargs, *funciargs, *funcresult, *protrftypes;
    char *prokind, *provolatile, *proisstrict, *prosecdef;
    char *proleakproof, *proconfig, *procost, *prorows;
    char *prosupport, *proparallel, *lanname;
    char **configitems = NULL;
    int nconfigitems = 0;
    const char *keyword;

    // Skip in data-only dumps
    if (dopt->dataOnly)
        return;

    // Initialize buffers
    query = createPQExpBuffer();
    q = createPQExpBuffer();
    delqry = createPQExpBuffer();
    asPart = createPQExpBuffer();

    // Set up prepared statement for function details (if not already prepared)
    if (!fout->is_prepared[PREPQUERY_DUMPFUNC]) {
        // Build version-specific query for function metadata
        appendPQExpBufferStr(query, "PREPARE dumpFunc(pg_catalog.oid) AS\n");
        appendPQExpBufferStr(query, "SELECT proretset, prosrc, probin, provolatile, "
                             "proisstrict, prosecdef, lanname, proconfig, procost, prorows, "
                             "pg_catalog.pg_get_function_arguments(p.oid) AS funcargs, "
                             "pg_catalog.pg_get_function_identity_arguments(p.oid) AS funciargs, "
                             "pg_catalog.pg_get_function_result(p.oid) AS funcresult, "
                             "proleakproof, ");

        // Add version-specific fields
        if (fout->remoteVersion >= 90500)
            appendPQExpBufferStr(query, "array_to_string(protrftypes, ' ') AS protrftypes,\n");
        if (fout->remoteVersion >= 90600)
            appendPQExpBufferStr(query, "proparallel,\n");
        if (fout->remoteVersion >= 110000)
            appendPQExpBufferStr(query, "prokind,\n");
        if (fout->remoteVersion >= 120000)
            appendPQExpBufferStr(query, "prosupport,\n");
        if (fout->remoteVersion >= 140000)
            appendPQExpBufferStr(query, "pg_get_function_sqlbody(p.oid) AS prosqlbody\n");

        appendPQExpBufferStr(query, "FROM pg_catalog.pg_proc p, pg_catalog.pg_language l "
                             "WHERE p.oid = $1 AND l.oid = p.prolang");

        ExecuteSqlStatement(fout, query->data);
        fout->is_prepared[PREPQUERY_DUMPFUNC] = true;
    }

    // Execute query to get function details
    printfPQExpBuffer(query, "EXECUTE dumpFunc('%u')", finfo->dobj.catId.oid);
    res = ExecuteSqlQueryForSingleRow(fout, query->data);

    // Extract function properties from query result
    proretset = PQgetvalue(res, 0, PQfnumber(res, "proretset"));
    funcargs = PQgetvalue(res, 0, PQfnumber(res, "funcargs"));
    funciargs = PQgetvalue(res, 0, PQfnumber(res, "funciargs"));
    funcresult = PQgetvalue(res, 0, PQfnumber(res, "funcresult"));
    prokind = PQgetvalue(res, 0, PQfnumber(res, "prokind"));
    provolatile = PQgetvalue(res, 0, PQfnumber(res, "provolatile"));
    proisstrict = PQgetvalue(res, 0, PQfnumber(res, "proisstrict"));
    prosecdef = PQgetvalue(res, 0, PQfnumber(res, "prosecdef"));
    proleakproof = PQgetvalue(res, 0, PQfnumber(res, "proleakproof"));
    proconfig = PQgetvalue(res, 0, PQfnumber(res, "proconfig"));
    procost = PQgetvalue(res, 0, PQfnumber(res, "procost"));
    prorows = PQgetvalue(res, 0, PQfnumber(res, "prorows"));
    prosupport = PQgetvalue(res, 0, PQfnumber(res, "prosupport"));
    proparallel = PQgetvalue(res, 0, PQfnumber(res, "proparallel"));
    lanname = PQgetvalue(res, 0, PQfnumber(res, "lanname"));

    // Handle function source code (SQL body, binary+source, or source only)
    if (!PQgetisnull(res, 0, PQfnumber(res, "prosqlbody"))) {
        prosqlbody = PQgetvalue(res, 0, PQfnumber(res, "prosqlbody"));
        appendPQExpBufferStr(asPart, prosqlbody);
    } else {
        prosrc = PQgetvalue(res, 0, PQfnumber(res, "prosrc"));
        probin = PQgetvalue(res, 0, PQfnumber(res, "probin"));

        appendPQExpBufferStr(asPart, "AS ");
        if (probin[0] != '\0') {
            appendStringLiteralAH(asPart, probin, fout);
            if (prosrc[0] != '\0') {
                appendPQExpBufferStr(asPart, ", ");
                if (dopt->disable_dollar_quoting ||
                    (strchr(prosrc, '\'') == NULL && strchr(prosrc, '\\') == NULL))
                    appendStringLiteralAH(asPart, prosrc, fout);
                else
                    appendStringLiteralDQ(asPart, prosrc, NULL);
            }
        } else {
            if (dopt->disable_dollar_quoting)
                appendStringLiteralAH(asPart, prosrc, fout);
            else
                appendStringLiteralDQ(asPart, prosrc, NULL);
        }
    }

    // Parse configuration parameters
    if (*proconfig) {
        if (!parsePGArray(proconfig, &configitems, &nconfigitems))
            pg_fatal("could not parse %s array", "proconfig");
    }

    // Generate function signatures
    funcfullsig = format_function_arguments(finfo, funcargs, false);
    funcsig = format_function_arguments(finfo, funciargs, false);
    funcsig_tag = format_function_signature(fout, finfo, false);
    qual_funcsig = psprintf("%s.%s", fmtId(finfo->dobj.namespace->dobj.name), funcsig);

    // Determine if this is a procedure or function
    keyword = (prokind[0] == PROKIND_PROCEDURE) ? "PROCEDURE" : "FUNCTION";

    // Build DROP statement
    appendPQExpBuffer(delqry, "DROP %s %s;\n", keyword, qual_funcsig);

    // Build CREATE statement
    appendPQExpBuffer(q, "CREATE %s %s.%s", keyword,
                      fmtId(finfo->dobj.namespace->dobj.name),
                      funcfullsig ? funcfullsig : funcsig);

    // Add return type (except for procedures)
    if (prokind[0] != PROKIND_PROCEDURE) {
        if (funcresult)
            appendPQExpBuffer(q, " RETURNS %s", funcresult);
        else
            appendPQExpBuffer(q, " RETURNS %s%s",
                              (proretset[0] == 't') ? "SETOF " : "",
                              getFormattedTypeName(fout, finfo->prorettype, zeroIsError));
    }

    // Add language
    appendPQExpBuffer(q, "\n    LANGUAGE %s", fmtId(lanname));

    // Add optional attributes
    if (prokind[0] == PROKIND_WINDOW)
        appendPQExpBufferStr(q, " WINDOW");

    if (provolatile[0] == PROVOLATILE_IMMUTABLE)
        appendPQExpBufferStr(q, " IMMUTABLE");
    else if (provolatile[0] == PROVOLATILE_STABLE)
        appendPQExpBufferStr(q, " STABLE");

    if (proisstrict[0] == 't')
        appendPQExpBufferStr(q, " STRICT");

    if (prosecdef[0] == 't')
        appendPQExpBufferStr(q, " SECURITY DEFINER");

    if (proleakproof[0] == 't')
        appendPQExpBufferStr(q, " LEAKPROOF");

    // Add cost if non-default
    if (strcmp(procost, "0") != 0) {
        bool is_c_lang = (strcmp(lanname, "internal") == 0 || strcmp(lanname, "c") == 0);
        char *default_cost = is_c_lang ? "1" : "100";
        if (strcmp(procost, default_cost) != 0)
            appendPQExpBuffer(q, " COST %s", procost);
    }

    // Add rows estimate for set-returning functions
    if (proretset[0] == 't' && strcmp(prorows, "0") != 0 && strcmp(prorows, "1000") != 0)
        appendPQExpBuffer(q, " ROWS %s", prorows);

    // Add support function if specified
    if (strcmp(prosupport, "-") != 0)
        appendPQExpBuffer(q, " SUPPORT %s", prosupport);

    // Add parallel mode if not unsafe
    if (proparallel[0] == PROPARALLEL_SAFE)
        appendPQExpBufferStr(q, " PARALLEL SAFE");
    else if (proparallel[0] == PROPARALLEL_RESTRICTED)
        appendPQExpBufferStr(q, " PARALLEL RESTRICTED");

    // Add configuration parameters
    for (int i = 0; i < nconfigitems; i++) {
        char *configitem = configitems[i];
        char *pos = strchr(configitem, '=');
        if (pos == NULL) continue;

        *pos++ = '\0';
        appendPQExpBuffer(q, "\n    SET %s TO ", fmtId(configitem));

        if (variable_is_guc_list_quote(configitem)) {
            // Handle list-quoted variables
            char **namelist;
            if (SplitGUCList(pos, ',', &namelist)) {
                for (char **nameptr = namelist; *nameptr; nameptr++) {
                    if (nameptr != namelist)
                        appendPQExpBufferStr(q, ", ");
                    appendStringLiteralAH(q, *nameptr, fout);
                }
            }
            pg_free(namelist);
        } else {
            appendStringLiteralAH(q, pos, fout);
        }
    }

    // Add function body
    appendPQExpBuffer(q, "\n    %s;\n", asPart->data);

    // Handle extension dependencies and binary upgrade
    append_depends_on_extension(fout, q, &finfo->dobj, "pg_catalog.pg_proc", keyword, qual_funcsig);

    if (dopt->binary_upgrade)
        binary_upgrade_extension_member(q, &finfo->dobj, keyword, funcsig,
                                        finfo->dobj.namespace->dobj.name);

    // Archive the function
    if (finfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
        ArchiveEntry(fout, finfo->dobj.catId, finfo->dobj.dumpId,
                     ARCHIVE_OPTS(.tag = funcsig_tag,
                                  .namespace = finfo->dobj.namespace->dobj.name,
                                  .owner = finfo->rolname,
                                  .description = keyword,
                                  .section = finfo->postponed_def ? SECTION_POST_DATA : SECTION_PRE_DATA,
                                  .createStmt = q->data,
                                  .dropStmt = delqry->data));

    // Dump associated metadata
    if (finfo->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpComment(fout, keyword, funcsig, finfo->dobj.namespace->dobj.name,
                    finfo->rolname, finfo->dobj.catId, 0, finfo->dobj.dumpId);

    if (finfo->dobj.dump & DUMP_COMPONENT_SECLABEL)
        dumpSecLabel(fout, keyword, funcsig, finfo->dobj.namespace->dobj.name,
                     finfo->rolname, finfo->dobj.catId, 0, finfo->dobj.dumpId);

    if (finfo->dobj.dump & DUMP_COMPONENT_ACL)
        dumpACL(fout, finfo->dobj.dumpId, InvalidDumpId, keyword, funcsig, NULL,
                finfo->dobj.namespace->dobj.name, NULL, finfo->rolname, &finfo->dacl);

    // Cleanup
    PQclear(res);
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delqry);
    destroyPQExpBuffer(asPart);
    free(funcsig);
    free(funcfullsig);
    free(funcsig_tag);
    free(qual_funcsig);
    free(configitems);
}
```