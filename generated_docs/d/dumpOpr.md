# dumpOpr

## Location
[src/bin/pg_dump/pg_dump.c:12962-13180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L12962-L13180)

## Overview
Generates SQL DDL statements to recreate PostgreSQL user-defined operators, including all operator properties and associated functions during database dump operations.

## Definition
```c
static void dumpOpr(Archive *fout, const OprInfo *oprinfo)
```

## Detailed Description
This function creates SQL CREATE OPERATOR statements to recreate user-defined operators in PostgreSQL. It handles all operator properties including the implementation function, left and right operand types, commutator and negator operators, restriction and join selectivity functions, and operator characteristics like merge and hash join support. The function uses prepared statements for efficiency and handles different operator kinds (binary, left unary, right unary), though postfix operators are deprecated in PostgreSQL 14+.

Key responsibilities include:
- Building CREATE OPERATOR statements with complete operator specifications
- Handling different operator kinds: binary ('b'), left unary ('l'), and right unary ('r')
- Processing operator relationships (commutator, negator)
- Managing selectivity estimation functions (restriction and join)
- Formatting operator signatures for proper identification
- Generating corresponding DROP OPERATOR statements for clean replacements
- Handling deprecated postfix operator warnings

## Parameters / Member Variables
- `fout`: Archive structure containing dump context and output formatting information  
- `oprinfo`: OprInfo structure containing operator metadata including OID, name, implementation function, operand types, and various operator properties

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)
  - [convertRegProcReference](../c/convertRegProcReference.md)
  - [getFormattedOperatorName](../g/getFormattedOperatorName.md)
  - [fmtId](../f/fmtId.md)
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - pg_log_warning
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)
  - fmtQualifiedDumpable

## Notes and Other Information
- The function is skipped entirely during data-only dumps (when dopt->dataOnly is true)
- Invalid operators (those without valid oprcode) are silently skipped
- Uses prepared statements (PREPQUERY_DUMPOPR) for performance optimization when dumping multiple operators
- Postfix operators ('r' kind) generate warnings in PostgreSQL 14+ as they are no longer supported
- Operator signatures are formatted to include operand types, using "NONE" for missing operands in unary operators
- Function references are converted using convertRegProcReference for proper formatting
- Operator relationships (commutator, negator) are resolved and formatted using getFormattedOperatorName
- Memory management uses PostgreSQL's PQExpBuffer system with proper cleanup
- Binary upgrade scenarios are supported through extension member handling

## Simplified Source

```c
static void
dumpOpr(Archive *fout, const OprInfo *oprinfo)
{
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer query, q, delq, oprid, details;
    PGresult *res;
    char *oprkind, *oprcode, *oprleft, *oprright;
    char *oprcom, *oprnegate, *oprrest, *oprjoin;
    char *oprcanmerge, *oprcanhash;
    char *oprregproc, *oprref;

    // Skip in data-only dumps
    if (dopt->dataOnly)
        return;

    // Skip invalid operators
    if (!OidIsValid(oprinfo->oprcode))
        return;

    // Initialize buffers
    query = createPQExpBuffer();
    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    oprid = createPQExpBuffer();
    details = createPQExpBuffer();

    // Set up prepared statement for operator details (if not already prepared)
    if (!fout->is_prepared[PREPQUERY_DUMPOPR]) {
        appendPQExpBufferStr(query,
                             "PREPARE dumpOpr(pg_catalog.oid) AS\n"
                             "SELECT oprkind, "
                             "oprcode::pg_catalog.regprocedure, "
                             "oprleft::pg_catalog.regtype, "
                             "oprright::pg_catalog.regtype, "
                             "oprcom, oprnegate, "
                             "oprrest::pg_catalog.regprocedure, "
                             "oprjoin::pg_catalog.regprocedure, "
                             "oprcanmerge, oprcanhash "
                             "FROM pg_catalog.pg_operator "
                             "WHERE oid = $1");

        ExecuteSqlStatement(fout, query->data);
        fout->is_prepared[PREPQUERY_DUMPOPR] = true;
    }

    // Execute query to get operator details
    printfPQExpBuffer(query, "EXECUTE dumpOpr('%u')", oprinfo->dobj.catId.oid);
    res = ExecuteSqlQueryForSingleRow(fout, query->data);

    // Extract operator properties
    oprkind = PQgetvalue(res, 0, PQfnumber(res, "oprkind"));
    oprcode = PQgetvalue(res, 0, PQfnumber(res, "oprcode"));
    oprleft = PQgetvalue(res, 0, PQfnumber(res, "oprleft"));
    oprright = PQgetvalue(res, 0, PQfnumber(res, "oprright"));
    oprcom = PQgetvalue(res, 0, PQfnumber(res, "oprcom"));
    oprnegate = PQgetvalue(res, 0, PQfnumber(res, "oprnegate"));
    oprrest = PQgetvalue(res, 0, PQfnumber(res, "oprrest"));
    oprjoin = PQgetvalue(res, 0, PQfnumber(res, "oprjoin"));
    oprcanmerge = PQgetvalue(res, 0, PQfnumber(res, "oprcanmerge"));
    oprcanhash = PQgetvalue(res, 0, PQfnumber(res, "oprcanhash"));

    // Warn about deprecated postfix operators
    if (strcmp(oprkind, "r") == 0)
        pg_log_warning("postfix operators are not supported anymore (operator \"%s\")",
                       oprcode);

    // Build operator function specification
    oprregproc = convertRegProcReference(oprcode);
    if (oprregproc) {
        appendPQExpBuffer(details, "    FUNCTION = %s", oprregproc);
        free(oprregproc);
    }

    // Build operator signature
    appendPQExpBuffer(oprid, "%s (", oprinfo->dobj.name);

    // Handle left argument (binary and right unary operators)
    if (strcmp(oprkind, "r") == 0 || strcmp(oprkind, "b") == 0) {
        appendPQExpBuffer(details, ",\n    LEFTARG = %s", oprleft);
        appendPQExpBufferStr(oprid, oprleft);
    } else {
        appendPQExpBufferStr(oprid, "NONE");
    }

    // Handle right argument (binary and left unary operators)
    if (strcmp(oprkind, "l") == 0 || strcmp(oprkind, "b") == 0) {
        appendPQExpBuffer(details, ",\n    RIGHTARG = %s", oprright);
        appendPQExpBuffer(oprid, ", %s)", oprright);
    } else {
        appendPQExpBufferStr(oprid, ", NONE)");
    }

    // Add commutator operator
    oprref = getFormattedOperatorName(oprcom);
    if (oprref) {
        appendPQExpBuffer(details, ",\n    COMMUTATOR = %s", oprref);
        free(oprref);
    }

    // Add negator operator
    oprref = getFormattedOperatorName(oprnegate);
    if (oprref) {
        appendPQExpBuffer(details, ",\n    NEGATOR = %s", oprref);
        free(oprref);
    }

    // Add operator characteristics
    if (strcmp(oprcanmerge, "t") == 0)
        appendPQExpBufferStr(details, ",\n    MERGES");

    if (strcmp(oprcanhash, "t") == 0)
        appendPQExpBufferStr(details, ",\n    HASHES");

    // Add selectivity functions
    oprregproc = convertRegProcReference(oprrest);
    if (oprregproc) {
        appendPQExpBuffer(details, ",\n    RESTRICT = %s", oprregproc);
        free(oprregproc);
    }

    oprregproc = convertRegProcReference(oprjoin);
    if (oprregproc) {
        appendPQExpBuffer(details, ",\n    JOIN = %s", oprregproc);
        free(oprregproc);
    }

    // Build CREATE and DROP statements
    appendPQExpBuffer(delq, "DROP OPERATOR %s.%s;\n",
                      fmtId(oprinfo->dobj.namespace->dobj.name), oprid->data);

    appendPQExpBuffer(q, "CREATE OPERATOR %s.%s (\n%s\n);\n",
                      fmtId(oprinfo->dobj.namespace->dobj.name),
                      oprinfo->dobj.name, details->data);

    // Handle binary upgrade
    if (dopt->binary_upgrade)
        binary_upgrade_extension_member(q, &oprinfo->dobj, "OPERATOR", oprid->data,
                                        oprinfo->dobj.namespace->dobj.name);

    // Archive the operator
    if (oprinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
        ArchiveEntry(fout, oprinfo->dobj.catId, oprinfo->dobj.dumpId,
                     ARCHIVE_OPTS(.tag = oprinfo->dobj.name,
                                  .namespace = oprinfo->dobj.namespace->dobj.name,
                                  .owner = oprinfo->rolname,
                                  .description = "OPERATOR",
                                  .section = SECTION_PRE_DATA,
                                  .createStmt = q->data,
                                  .dropStmt = delq->data));

    // Dump operator comments
    if (oprinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpComment(fout, "OPERATOR", oprid->data,
                    oprinfo->dobj.namespace->dobj.name, oprinfo->rolname,
                    oprinfo->dobj.catId, 0, oprinfo->dobj.dumpId);

    // Cleanup
    PQclear(res);
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(oprid);
    destroyPQExpBuffer(details);
}
```