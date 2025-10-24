# dumpRule

## Location
[src/bin/pg_dump/pg_dump.c:18104-18270](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L18104-L18270)

## Overview
Dumps PostgreSQL rules, with special handling for view-defining ON SELECT rules that are treated as CREATE VIEW statements rather than separate rule objects.

## Definition

```c
static void
dumpRule(Archive *fout, const RuleInfo *rinfo)
```
## Detailed Description
The  function generates SQL statements for PostgreSQL rules, with sophisticated logic to handle different rule types. For ON SELECT rules that define views (ev_type == '1' and is_instead == true), it generates CREATE OR REPLACE VIEW statements instead of CREATE RULE statements, including view options and CHECK OPTION clauses. For regular rules, it uses pg_get_ruledef() to retrieve the complete rule definition. The function also handles rule replication firing semantics through ALTER TABLE ENABLE/DISABLE RULE commands when the rule's enabled state differs from the default ('O'). Non-separate rules (typically implicit view rules) are skipped entirely.

## Parameters / Member Variables
- `*fout`: Archive structure containing dump options and output methods
- `*rinfo`: RuleInfo structure containing rule metadata including rule table, event type, instead flag, enabled state, and separation flag
## Dependencies
- Functions called/Symbols referenced:
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [nonemptyReloptions](../n/nonemptyReloptions.md)
  - [appendReloptionsArrayAH](../a/appendReloptionsArrayAH.md)
  - [createViewAsClause](../c/createViewAsClause.md)
  - [createDummyViewAsClause](../c/createDummyViewAsClause.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [psprintf](../p/psprintf.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)

## Notes and Other Information
- Skips processing in data-only dump mode and for non-separate rules
- Distinguishes between view-defining rules (ON SELECT INSTEAD) and regular rules
- For views, uses CREATE OR REPLACE VIEW to handle dummy view replacement during restore
- Handles three rule enabled states: 'A' (always), 'R' (replica), 'D' (disabled), with 'O' being default
- Creates archive entries in SECTION_POST_DATA to ensure rules are created after their dependent tables
- For view rules, DROP statements use CREATE OR REPLACE VIEW with dummy content instead of DROP RULE
- Preserves view reloptions and check options when dumping view-defining rules

## Simplified Source

```c
static void
dumpRule(Archive *fout, const RuleInfo *rinfo)
{
    DumpOptions *dopt = fout->dopt;
    TableInfo *tbinfo = rinfo->ruletable;
    bool is_view;
    PQExpBuffer query, cmd, delcmd, ruleprefix;
    char *qtabname;
    PGresult *res;
    char *tag;

    // Skip in data-only mode or non-separate rules
    if (dopt->dataOnly || !rinfo->separate)
        return;

    // Determine if this is a view-defining rule
    is_view = (rinfo->ev_type == '1' && rinfo->is_instead);

    query = createPQExpBuffer();
    cmd = createPQExpBuffer();
    delcmd = createPQExpBuffer();
    ruleprefix = createPQExpBuffer();

    qtabname = pg_strdup(fmtId(tbinfo->dobj.name));

    if (is_view) {
        // Handle view-defining rules: CREATE OR REPLACE VIEW
        PQExpBuffer result;

        appendPQExpBuffer(cmd, "CREATE OR REPLACE VIEW %s", fmtQualifiedDumpable(tbinfo));

        // Add view options if present
        if (nonemptyReloptions(tbinfo->reloptions)) {
            appendPQExpBufferStr(cmd, " WITH (");
            appendReloptionsArrayAH(cmd, tbinfo->reloptions, "", fout);
            appendPQExpBufferChar(cmd, ')');
        }

        // Add view definition
        result = createViewAsClause(fout, tbinfo);
        appendPQExpBuffer(cmd, " AS\n%s", result->data);
        destroyPQExpBuffer(result);

        // Add check option if present
        if (tbinfo->checkoption != NULL)
            appendPQExpBuffer(cmd, "\n  WITH %s CHECK OPTION", tbinfo->checkoption);

        appendPQExpBufferStr(cmd, ";\n");
    } else {
        // Handle regular rules: use pg_get_ruledef
        appendPQExpBuffer(query, "SELECT pg_catalog.pg_get_ruledef('%u'::pg_catalog.oid)",
                         rinfo->dobj.catId.oid);

        res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

        if (PQntuples(res) != 1)
            pg_fatal("query to get rule \"%s\" for table \"%s\" failed: wrong number of rows returned",
                    rinfo->dobj.name, tbinfo->dobj.name);

        printfPQExpBuffer(cmd, "%s\n", PQgetvalue(res, 0, 0));
        PQclear(res);
    }

    // Handle non-default rule enabled states
    if (rinfo->ev_enabled != 'O') {
        appendPQExpBuffer(cmd, "ALTER TABLE %s ", fmtQualifiedDumpable(tbinfo));
        switch (rinfo->ev_enabled) {
            case 'A':
                appendPQExpBuffer(cmd, "ENABLE ALWAYS RULE %s;\n", fmtId(rinfo->dobj.name));
                break;
            case 'R':
                appendPQExpBuffer(cmd, "ENABLE REPLICA RULE %s;\n", fmtId(rinfo->dobj.name));
                break;
            case 'D':
                appendPQExpBuffer(cmd, "DISABLE RULE %s;\n", fmtId(rinfo->dobj.name));
                break;
        }
    }

    // Create DROP statement
    if (is_view) {
        // For views, create dummy view for DROP
        PQExpBuffer result;
        appendPQExpBuffer(delcmd, "CREATE OR REPLACE VIEW %s", fmtQualifiedDumpable(tbinfo));
        result = createDummyViewAsClause(fout, tbinfo);
        appendPQExpBuffer(delcmd, " AS\n%s;\n", result->data);
        destroyPQExpBuffer(result);
    } else {
        // Regular rule DROP
        appendPQExpBuffer(delcmd, "DROP RULE %s ", fmtId(rinfo->dobj.name));
        appendPQExpBuffer(delcmd, "ON %s;\n", fmtQualifiedDumpable(tbinfo));
    }

    // Build comment prefix
    appendPQExpBuffer(ruleprefix, "RULE %s ON", fmtId(rinfo->dobj.name));

    tag = psprintf("%s %s", tbinfo->dobj.name, rinfo->dobj.name);

    // Create archive entry
    if (rinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
        ArchiveEntry(fout, rinfo->dobj.catId, rinfo->dobj.dumpId,
                   ARCHIVE_OPTS(.tag = tag,
                               .namespace = tbinfo->dobj.namespace->dobj.name,
                               .owner = tbinfo->rolname,
                               .description = "RULE",
                               .section = SECTION_POST_DATA,
                               .createStmt = cmd->data,
                               .dropStmt = delcmd->data));

    // Dump rule comments
    if (rinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpComment(fout, ruleprefix->data, qtabname,
                   tbinfo->dobj.namespace->dobj.name,
                   tbinfo->rolname,
                   rinfo->dobj.catId, 0, rinfo->dobj.dumpId);

    free(tag);
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(cmd);
    destroyPQExpBuffer(delcmd);
    destroyPQExpBuffer(ruleprefix);
    free(qtabname);
}
```