# dumpTableSchema

## Location
[src/bin/pg_dump/pg_dump.c:15946-16807](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L15946-L16807)

## Overview
Generates the SQL declaration (schema definition) for a user-defined table, view, materialized view, foreign table, or partitioned table, handling all structural elements without data content.

## Definition

```c
static void
dumpTableSchema(Archive *fout, const TableInfo *tbinfo)
```
## Detailed Description
This is a comprehensive function that constructs CREATE TABLE, CREATE VIEW, CREATE MATERIALIZED VIEW, or CREATE FOREIGN TABLE statements with all associated properties. It handles diverse table types and their specific requirements:

- **Views**: Creates standard or dummy views with column specifications and CHECK OPTION clauses
- **Tables**: Regular, partitioned, foreign, and materialized tables with complete attribute definitions
- **Binary upgrade mode**: Special handling for maintaining exact compatibility during upgrades
- **Inheritance**: Processes parent-child relationships and inherited constraints  
- **Advanced features**: Handles storage parameters, statistics targets, compression, replica identity, row-level security, and tablespaces

The function generates both CREATE and DROP statements, manages column properties (types, defaults, NOT NULL, collation), processes constraints, and handles special cases for dropped columns and binary upgrade scenarios.

## Parameters / Member Variables
- `*fout`: Archive context containing dump configuration and output handling
- `*tbinfo`: Complete table metadata including columns, constraints, inheritance, and storage properties
## Dependencies
- Functions called/Symbols referenced:
  - [createDummyViewAsClause](../c/createDummyViewAsClause.md)
  - [createViewAsClause](../c/createViewAsClause.md)
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [binary_upgrade_set_type_oids_by_rel](../b/binary_upgrade_set_type_oids_by_rel.md)
  - [binary_upgrade_set_pg_class_oids](../b/binary_upgrade_set_pg_class_oids.md)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)
  - [getFormattedTypeName](../g/getFormattedTypeName.md)
  - [shouldPrintColumn](../s/shouldPrintColumn.md)
  - [findCollationByOid](../f/findCollationByOid.md)
  - [appendReloptionsArrayAH](../a/appendReloptionsArrayAH.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpTableComment](dumpTableComment.md)
  - [dumpTableSecLabel](dumpTableSecLabel.md)
- Types referenced:
  - [Archive](../A/Archive.md)
  - [TableInfo](../T/TableInfo.md)
  - DumpOptions
  - [CollInfo](../C/CollInfo.md)
  - [ConstraintInfo](../C/ConstraintInfo.md)
  - PQExpBuffer
- Called from:
  - [dumpTable](dumpTable.md)

## Notes and Other Information
- Handles complex inheritance hierarchies and constraint propagation
- Special logic for binary upgrade mode to preserve exact database structure
- Manages tablespace assignments and access method specifications
- Processes replica identity settings for logical replication
- Creates appropriate dependency relationships for proper restore ordering
- Handles both regular and dummy view creation to resolve circular dependencies
- Supports PostgreSQL-specific features like row-level security and generated columns

## Simplified Source

```c
static void
dumpTableSchema(Archive *fout, const TableInfo *tbinfo)
{
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer q = createPQExpBuffer();
    PQExpBuffer delq = createPQExpBuffer();
    char *qrelname = pg_strdup(fmtId(tbinfo->dobj.name));
    char *qualrelname = pg_strdup(fmtQualifiedDumpable(tbinfo));

    // Binary upgrade setup
    if (dopt->binary_upgrade)
        binary_upgrade_set_type_oids_by_rel(fout, q, tbinfo);

    // Handle views
    if (tbinfo->relkind == RELKIND_VIEW)
    {
        PQExpBuffer result;

        appendPQExpBuffer(delq, "DROP VIEW %s;\n", qualrelname);

        if (dopt->binary_upgrade)
            binary_upgrade_set_pg_class_oids(fout, q, tbinfo->dobj.catId.oid, false);

        appendPQExpBuffer(q, "CREATE VIEW %s", qualrelname);

        // Create view AS clause (real or dummy)
        if (tbinfo->dummy_view)
            result = createDummyViewAsClause(fout, tbinfo);
        else
        {
            // Add reloptions if present
            if (nonemptyReloptions(tbinfo->reloptions))
            {
                appendPQExpBufferStr(q, " WITH (");
                appendReloptionsArrayAH(q, tbinfo->reloptions, "", fout);
                appendPQExpBufferChar(q, ')');
            }
            result = createViewAsClause(fout, tbinfo);
        }

        appendPQExpBuffer(q, " AS\n%s", result->data);
        destroyPQExpBuffer(result);

        // Add check option if specified
        if (tbinfo->checkoption != NULL && !tbinfo->dummy_view)
            appendPQExpBuffer(q, "\n  WITH %s CHECK OPTION", tbinfo->checkoption);
        appendPQExpBufferStr(q, ";\n");
    }
    else
    {
        // Handle tables, foreign tables, materialized views
        const char *reltypename;
        char *foreign = "";

        // Determine relation type and fetch type-specific data
        switch (tbinfo->relkind)
        {
            case RELKIND_PARTITIONED_TABLE:
                reltypename = "TABLE";
                // Get partition key definition
                break;
            case RELKIND_FOREIGN_TABLE:
                reltypename = "FOREIGN TABLE";
                foreign = "FOREIGN ";
                // Get foreign server and options
                break;
            case RELKIND_MATVIEW:
                reltypename = "MATERIALIZED VIEW";
                break;
            default:
                reltypename = "TABLE";
                break;
        }

        appendPQExpBuffer(delq, "DROP %s %s;\n", reltypename, qualrelname);

        if (dopt->binary_upgrade)
            binary_upgrade_set_pg_class_oids(fout, q, tbinfo->dobj.catId.oid, false);

        // Create table statement
        appendPQExpBuffer(q, "CREATE %s%s %s",
                         tbinfo->relpersistence == RELPERSISTENCE_UNLOGGED ? "UNLOGGED " : "",
                         reltypename, qualrelname);

        // Handle typed tables
        if (OidIsValid(tbinfo->reloftype) && !dopt->binary_upgrade)
            appendPQExpBuffer(q, " OF %s",
                            getFormattedTypeName(fout, tbinfo->reloftype, zeroIsError));

        // Dump table attributes for non-materialized views
        if (tbinfo->relkind != RELKIND_MATVIEW)
        {
            int actual_atts = 0;

            // Process each attribute
            for (int j = 0; j < tbinfo->numatts; j++)
            {
                if (shouldPrintColumn(dopt, tbinfo, j))
                {
                    // Format column definition with type, defaults, constraints
                    if (actual_atts == 0)
                        appendPQExpBufferStr(q, " (");
                    else
                        appendPQExpBufferChar(q, ',');
                    appendPQExpBufferStr(q, "\n    ");
                    actual_atts++;

                    // Add column name and type
                    appendPQExpBufferStr(q, fmtId(tbinfo->attnames[j]));
                    if (!tbinfo->attisdropped[j])
                    {
                        if (dopt->binary_upgrade || !OidIsValid(tbinfo->reloftype))
                            appendPQExpBuffer(q, " %s", tbinfo->atttypnames[j]);

                        // Add defaults, NOT NULL, collation as needed
                        // (simplified - detailed logic omitted)
                    }
                }
            }

            // Add table constraints
            for (int j = 0; j < tbinfo->ncheck; j++)
            {
                ConstraintInfo *constr = &(tbinfo->checkexprs[j]);
                if (!constr->separate && (constr->conislocal || tbinfo->ispartition))
                {
                    if (actual_atts == 0)
                        appendPQExpBufferStr(q, " (\n    ");
                    else
                        appendPQExpBufferStr(q, ",\n    ");
                    appendPQExpBuffer(q, "CONSTRAINT %s %s",
                                    fmtId(constr->dobj.name), constr->condef);
                    actual_atts++;
                }
            }

            if (actual_atts)
                appendPQExpBufferStr(q, "\n)");
        }

        // Add inheritance, partitioning, foreign table options
        // Add reloptions if present
        // Handle materialized view AS clause

        appendPQExpBufferStr(q, ";\n");
    }

    // Add replica identity, row security, extension dependencies
    // Handle per-column properties (statistics, storage, compression)

    // Create archive entry
    if (tbinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
    {
        ArchiveEntry(fout, tbinfo->dobj.catId, tbinfo->dobj.dumpId,
                    ARCHIVE_OPTS(.tag = tbinfo->dobj.name,
                                .namespace = tbinfo->dobj.namespace->dobj.name,
                                .owner = tbinfo->rolname,
                                .description = reltypename,
                                .createStmt = q->data,
                                .dropStmt = delq->data));
    }

    // Dump comments and security labels
    if (tbinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpTableComment(fout, tbinfo, reltypename);

    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    free(qrelname);
    free(qualrelname);
}
```