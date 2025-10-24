# dumpAgg

## Location
[src/bin/pg_dump/pg_dump.c:14227-14586](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L14227-L14586)

## Overview
Writes out a single aggregate function definition, generating CREATE AGGREGATE SQL statements with all necessary parameters including state functions, final functions, parallel options, and moving aggregates support.

## Definition

```c
static void
dumpAgg(Archive *fout, const AggInfo *agginfo)
```
## Detailed Description
The  function generates SQL commands to recreate aggregate functions during database dumps. It handles the complexity of PostgreSQL's aggregate definition syntax by constructing comprehensive CREATE AGGREGATE statements with all supported options:

- **Basic components**: State function (SFUNC), state type (STYPE), final function (FINALFUNC)
- **Advanced features**: Combine/serialize/deserialize functions for parallel aggregation
- **Moving aggregates**: Forward/inverse state functions (MSFUNC/MINVFUNC) for window functions
- **Optimization settings**: Parallel safety, state space estimation, function modify behavior
- **Special aggregate types**: Hypothetical aggregates, ordered-set aggregates

The function uses prepared statements for efficiency and includes extensive version compatibility handling across PostgreSQL 9.4+, 9.6+, and 11.0+ to manage evolving aggregate features. It processes aggregate metadata from pg_aggregate and pg_proc catalogs to generate complete aggregate definitions.

## Parameters / Member Variables
- `*fout`: Archive structure containing dump options and output methods
- `*agginfo`: AggInfo structure containing aggregate function metadata including OID, name, namespace, owner, and function details
## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)  
  - [format_function_arguments](../f/format_function_arguments.md)
  - [format_aggregate_signature](../f/format_aggregate_signature.md)
  - [format_function_signature](../f/format_function_signature.md)
  - [getFormattedOperatorName](../g/getFormattedOperatorName.md)
  - appendStringLiteralAH
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [dumpSecLabel](dumpSecLabel.md)
  - [dumpACL](dumpACL.md)
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)

## Notes and Other Information
- Only operates in schema dump mode (skipped when dopt->dataOnly is true)
- Uses prepared statements (PREPQUERY_DUMPAGG) for query optimization
- Handles version differences for aggregate features introduced over time
- Generates both identity signatures for DROP and full signatures for CREATE statements
- Special handling for ACL dumps using function syntax (no native GRANT ON AGGREGATE)
- Supports binary upgrade scenarios with extension membership
- Includes comprehensive validation and error handling for aggregate parameters
- Manages complex parameter combinations for different aggregate types and PostgreSQL versions

## Simplified Source

```c
static void
dumpAgg(Archive *fout, const AggInfo *agginfo)
{
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer query, q, delq, details;
    char *aggsig, *aggfullsig = NULL, *aggsig_tag;
    PGresult *res;
    const char *aggtransfn, *aggfinalfn, *aggtranstype;
    const char *agginitval, *aggminitval;
    char aggkind, defaultfinalmodify;

    // Skip in data-only mode
    if (dopt->dataOnly)
        return;

    // Initialize buffers
    query = createPQExpBuffer();
    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    details = createPQExpBuffer();

    // Set up prepared statement if not already done
    if (!fout->is_prepared[PREPQUERY_DUMPAGG]) {
        // Build version-aware query for aggregate properties
        appendPQExpBufferStr(query, "PREPARE dumpAgg(pg_catalog.oid) AS\n");

        // Basic aggregate properties
        appendPQExpBufferStr(query,
                            "SELECT aggtransfn, aggfinalfn, "
                            "aggtranstype::pg_catalog.regtype, agginitval, "
                            "aggsortop, "
                            "pg_catalog.pg_get_function_arguments(p.oid) AS funcargs, "
                            "pg_catalog.pg_get_function_identity_arguments(p.oid) AS funciargs, ");

        // Add version-specific fields for moving aggregates (9.4+)
        if (fout->remoteVersion >= 90400)
            appendPQExpBufferStr(query,
                                "aggkind, aggmtransfn, aggminvtransfn, aggmfinalfn, "
                                "aggmtranstype::pg_catalog.regtype, "
                                "aggfinalextra, aggmfinalextra, "
                                "aggtransspace, aggmtransspace, aggminitval, ");
        else
            appendPQExpBufferStr(query,
                                "'n' AS aggkind, '-' AS aggmtransfn, "
                                "'-' AS aggminvtransfn, '-' AS aggmfinalfn, "
                                "0 AS aggmtranstype, false AS aggfinalextra, "
                                "false AS aggmfinalextra, 0 AS aggtransspace, "
                                "0 AS aggmtransspace, NULL AS aggminitval, ");

        // Add parallel support fields (9.6+)
        if (fout->remoteVersion >= 90600)
            appendPQExpBufferStr(query,
                                "aggcombinefn, aggserialfn, aggdeserialfn, proparallel, ");
        else
            appendPQExpBufferStr(query,
                                "'-' AS aggcombinefn, '-' AS aggserialfn, "
                                "'-' AS aggdeserialfn, 'u' AS proparallel, ");

        // Add finalfunc modify behavior (11.0+)
        if (fout->remoteVersion >= 110000)
            appendPQExpBufferStr(query, "aggfinalmodify, aggmfinalmodify\n");
        else
            appendPQExpBufferStr(query, "'0' AS aggfinalmodify, '0' AS aggmfinalmodify\n");

        appendPQExpBufferStr(query,
                            "FROM pg_catalog.pg_aggregate a, pg_catalog.pg_proc p "
                            "WHERE a.aggfnoid = p.oid AND p.oid = $1");

        ExecuteSqlStatement(fout, query->data);
        fout->is_prepared[PREPQUERY_DUMPAGG] = true;
    }

    // Execute prepared query
    printfPQExpBuffer(query, "EXECUTE dumpAgg('%u')", agginfo->aggfn.dobj.catId.oid);
    res = ExecuteSqlQueryForSingleRow(fout, query->data);

    // Extract basic aggregate properties
    aggtransfn = PQgetvalue(res, 0, PQfnumber(res, "aggtransfn"));
    aggfinalfn = PQgetvalue(res, 0, PQfnumber(res, "aggfinalfn"));
    aggtranstype = PQgetvalue(res, 0, PQfnumber(res, "aggtranstype"));
    agginitval = PQgetvalue(res, 0, PQfnumber(res, "agginitval"));
    aggkind = PQgetvalue(res, 0, PQfnumber(res, "aggkind"))[0];

    // Build function signatures
    char *funcargs = PQgetvalue(res, 0, PQfnumber(res, "funcargs"));
    char *funciargs = PQgetvalue(res, 0, PQfnumber(res, "funciargs"));
    aggfullsig = format_function_arguments(&agginfo->aggfn, funcargs, true);
    aggsig = format_function_arguments(&agginfo->aggfn, funciargs, true);
    aggsig_tag = format_aggregate_signature(agginfo, fout, false);

    // Start building CREATE AGGREGATE statement
    appendPQExpBuffer(details, "    SFUNC = %s,\n    STYPE = %s",
                      aggtransfn, aggtranstype);

    // Add state space if specified
    const char *aggtransspace = PQgetvalue(res, 0, PQfnumber(res, "aggtransspace"));
    if (strcmp(aggtransspace, "0") != 0)
        appendPQExpBuffer(details, ",\n    SSPACE = %s", aggtransspace);

    // Add initial condition if present
    if (!PQgetisnull(res, 0, PQfnumber(res, "agginitval"))) {
        appendPQExpBufferStr(details, ",\n    INITCOND = ");
        appendStringLiteralAH(details, agginitval, fout);
    }

    // Add final function if present
    if (strcmp(aggfinalfn, "-") != 0) {
        appendPQExpBuffer(details, ",\n    FINALFUNC = %s", aggfinalfn);

        bool aggfinalextra = (PQgetvalue(res, 0, PQfnumber(res, "aggfinalextra"))[0] == 't');
        if (aggfinalextra)
            appendPQExpBufferStr(details, ",\n    FINALFUNC_EXTRA");
    }

    // Add parallel combine/serialize functions if present
    const char *aggcombinefn = PQgetvalue(res, 0, PQfnumber(res, "aggcombinefn"));
    if (strcmp(aggcombinefn, "-") != 0)
        appendPQExpBuffer(details, ",\n    COMBINEFUNC = %s", aggcombinefn);

    // Add moving aggregate functions if present
    const char *aggmtransfn = PQgetvalue(res, 0, PQfnumber(res, "aggmtransfn"));
    if (strcmp(aggmtransfn, "-") != 0) {
        const char *aggminvtransfn = PQgetvalue(res, 0, PQfnumber(res, "aggminvtransfn"));
        const char *aggmtranstype = PQgetvalue(res, 0, PQfnumber(res, "aggmtranstype"));
        appendPQExpBuffer(details, ",\n    MSFUNC = %s,\n    MINVFUNC = %s,\n    MSTYPE = %s",
                         aggmtransfn, aggminvtransfn, aggmtranstype);
    }

    // Add sort operator if present
    const char *aggsortop = PQgetvalue(res, 0, PQfnumber(res, "aggsortop"));
    char *aggsortconvop = getFormattedOperatorName(aggsortop);
    if (aggsortconvop) {
        appendPQExpBuffer(details, ",\n    SORTOP = %s", aggsortconvop);
        free(aggsortconvop);
    }

    // Add hypothetical flag if needed
    if (aggkind == AGGKIND_HYPOTHETICAL)
        appendPQExpBufferStr(details, ",\n    HYPOTHETICAL");

    // Add parallel safety if not unsafe
    const char *proparallel = PQgetvalue(res, 0, PQfnumber(res, "proparallel"));
    if (proparallel[0] == PROPARALLEL_SAFE)
        appendPQExpBufferStr(details, ",\n    PARALLEL = safe");
    else if (proparallel[0] == PROPARALLEL_RESTRICTED)
        appendPQExpBufferStr(details, ",\n    PARALLEL = restricted");

    // Build CREATE and DROP statements
    appendPQExpBuffer(delq, "DROP AGGREGATE %s.%s;\n",
                      fmtId(agginfo->aggfn.dobj.namespace->dobj.name), aggsig);

    appendPQExpBuffer(q, "CREATE AGGREGATE %s.%s (\n%s\n);\n",
                      fmtId(agginfo->aggfn.dobj.namespace->dobj.name),
                      aggfullsig ? aggfullsig : aggsig, details->data);

    // Handle binary upgrade
    if (dopt->binary_upgrade)
        binary_upgrade_extension_member(q, &agginfo->aggfn.dobj,
                                       "AGGREGATE", aggsig,
                                       agginfo->aggfn.dobj.namespace->dobj.name);

    // Register with archive
    if (agginfo->aggfn.dobj.dump & DUMP_COMPONENT_DEFINITION)
        ArchiveEntry(fout, agginfo->aggfn.dobj.catId, agginfo->aggfn.dobj.dumpId,
                    ARCHIVE_OPTS(.tag = aggsig_tag,
                                .namespace = agginfo->aggfn.dobj.namespace->dobj.name,
                                .owner = agginfo->aggfn.rolname,
                                .description = "AGGREGATE",
                                .section = SECTION_PRE_DATA,
                                .createStmt = q->data,
                                .dropStmt = delq->data));

    // Dump comments and security labels
    if (agginfo->aggfn.dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpComment(fout, "AGGREGATE", aggsig,
                   agginfo->aggfn.dobj.namespace->dobj.name,
                   agginfo->aggfn.rolname,
                   agginfo->aggfn.dobj.catId, 0, agginfo->aggfn.dobj.dumpId);

    if (agginfo->aggfn.dobj.dump & DUMP_COMPONENT_SECLABEL)
        dumpSecLabel(fout, "AGGREGATE", aggsig,
                    agginfo->aggfn.dobj.namespace->dobj.name,
                    agginfo->aggfn.rolname,
                    agginfo->aggfn.dobj.catId, 0, agginfo->aggfn.dobj.dumpId);

    // Dump ACL using function signature (no GRANT ON AGGREGATE syntax)
    free(aggsig);
    aggsig = format_function_signature(fout, &agginfo->aggfn, true);

    if (agginfo->aggfn.dobj.dump & DUMP_COMPONENT_ACL)
        dumpACL(fout, agginfo->aggfn.dobj.dumpId, InvalidDumpId,
               "FUNCTION", aggsig, NULL,
               agginfo->aggfn.dobj.namespace->dobj.name,
               NULL, agginfo->aggfn.rolname, &agginfo->aggfn.dacl);

    // Cleanup
    free(aggsig);
    free(aggfullsig);
    free(aggsig_tag);
    PQclear(res);
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(details);
}
```