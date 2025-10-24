# dumpSequence

## Location
[src/bin/pg_dump/pg_dump.c:17576-17842](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L17576-L17842)

## Overview
Writes the SQL declaration (not data) of one user-defined sequence to the dump output, handling both regular sequences and identity sequences.

## Definition

```c
static void
dumpSequence(Archive *fout, const TableInfo *tbinfo)
```
## Detailed Description
The  function generates SQL CREATE SEQUENCE statements for PostgreSQL sequences. It extracts sequence metadata from either  (PostgreSQL 10+) or the sequence relation itself (older versions) and constructs appropriate DDL statements. The function handles various sequence types (smallint, integer, bigint), calculates default min/max values based on sequence type and increment direction, and supports both standalone sequences and identity sequences. For identity sequences, it generates ALTER TABLE...ADD GENERATED statements instead of CREATE SEQUENCE. The function also handles sequence ownership relationships, comments, and security labels.

## Parameters / Member Variables
- `*fout`: Archive structure containing dump options and output methods
- `*tbinfo`: TableInfo structure containing sequence metadata including OID, name, ownership information, and identity sequence status
## Dependencies
- Functions called/Symbols referenced:
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [findTableByOid](../f/findTableByOid.md)
  - [binary_upgrade_set_pg_class_oids](../b/binary_upgrade_set_pg_class_oids.md)
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [dumpSecLabel](dumpSecLabel.md)
  - [createPQExpBuffer](../c/createPQExpBuffer.md)/resetPQExpBuffer/destroyPQExpBuffer
- Called from (representative examples):
  - [dumpTable](dumpTable.md)

## Notes and Other Information
- Supports PostgreSQL version compatibility by using different metadata sources (pg_sequence vs sequence relation)
- Handles three sequence data types: smallint, integer, and bigint with appropriate default limits
- Identity sequences are treated specially and integrated into ALTER TABLE statements
- Sequence ownership (OWNED BY) is handled as a separate archive entry to ensure proper dependency ordering
- Binary upgrade mode preserves OIDs for pg_class entries
- Comments and security labels are dumped as separate components if enabled

## Simplified Source

```c
static void
dumpSequence(Archive *fout, const TableInfo *tbinfo)
{
    DumpOptions *dopt = fout->dopt;
    PGresult *res;
    char *startv, *incby, *maxv, *minv, *cache, *seqtype;
    bool cycled, is_ascending;
    int64 default_minv, default_maxv;
    char bufm[32], bufx[32];
    PQExpBuffer query = createPQExpBuffer();
    PQExpBuffer delqry = createPQExpBuffer();
    char *qseqname;
    TableInfo *owning_tab = NULL;

    qseqname = pg_strdup(fmtId(tbinfo->dobj.name));

    // Query sequence metadata (version-dependent)
    if (fout->remoteVersion >= 100000) {
        appendPQExpBuffer(query,
                         "SELECT format_type(seqtypid, NULL), "
                         "seqstart, seqincrement, "
                         "seqmax, seqmin, "
                         "seqcache, seqcycle "
                         "FROM pg_catalog.pg_sequence "
                         "WHERE seqrelid = '%u'::oid",
                         tbinfo->dobj.catId.oid);
    } else {
        appendPQExpBuffer(query,
                         "SELECT 'bigint' AS sequence_type, "
                         "start_value, increment_by, max_value, min_value, "
                         "cache_value, is_cycled FROM %s",
                         fmtQualifiedDumpable(tbinfo));
    }

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    if (PQntuples(res) != 1)
        pg_fatal("query to get data of sequence \"%s\" returned %d rows (expected 1)",
                tbinfo->dobj.name, PQntuples(res));

    // Extract sequence parameters
    seqtype = PQgetvalue(res, 0, 0);
    startv = PQgetvalue(res, 0, 1);
    incby = PQgetvalue(res, 0, 2);
    maxv = PQgetvalue(res, 0, 3);
    minv = PQgetvalue(res, 0, 4);
    cache = PQgetvalue(res, 0, 5);
    cycled = (strcmp(PQgetvalue(res, 0, 6), "t") == 0);

    // Calculate default limits based on sequence type
    is_ascending = (incby[0] != '-');
    if (strcmp(seqtype, "smallint") == 0) {
        default_minv = is_ascending ? 1 : PG_INT16_MIN;
        default_maxv = is_ascending ? PG_INT16_MAX : -1;
    } else if (strcmp(seqtype, "integer") == 0) {
        default_minv = is_ascending ? 1 : PG_INT32_MIN;
        default_maxv = is_ascending ? PG_INT32_MAX : -1;
    } else if (strcmp(seqtype, "bigint") == 0) {
        default_minv = is_ascending ? 1 : PG_INT64_MIN;
        default_maxv = is_ascending ? PG_INT64_MAX : -1;
    } else {
        pg_fatal("unrecognized sequence type: %s", seqtype);
        default_minv = default_maxv = 0;
    }

    // Convert limits to strings and check if they're defaults
    snprintf(bufm, sizeof(bufm), INT64_FORMAT, default_minv);
    snprintf(bufx, sizeof(bufx), INT64_FORMAT, default_maxv);

    if (strcmp(minv, bufm) == 0) minv = NULL;
    if (strcmp(maxv, bufx) == 0) maxv = NULL;

    // Create DROP statement for non-identity sequences
    if (!tbinfo->is_identity_sequence) {
        appendPQExpBuffer(delqry, "DROP SEQUENCE %s;\n", fmtQualifiedDumpable(tbinfo));
    }

    resetPQExpBuffer(query);

    // Handle identity sequences vs regular sequences
    if (tbinfo->is_identity_sequence) {
        owning_tab = findTableByOid(tbinfo->owning_tab);

        appendPQExpBuffer(query, "ALTER TABLE %s ", fmtQualifiedDumpable(owning_tab));
        appendPQExpBuffer(query, "ALTER COLUMN %s ADD GENERATED ",
                         fmtId(owning_tab->attnames[tbinfo->owning_col - 1]));

        if (owning_tab->attidentity[tbinfo->owning_col - 1] == ATTRIBUTE_IDENTITY_ALWAYS)
            appendPQExpBufferStr(query, "ALWAYS");
        else if (owning_tab->attidentity[tbinfo->owning_col - 1] == ATTRIBUTE_IDENTITY_BY_DEFAULT)
            appendPQExpBufferStr(query, "BY DEFAULT");

        appendPQExpBuffer(query, " AS IDENTITY (\n    SEQUENCE NAME %s\n", fmtQualifiedDumpable(tbinfo));
    } else {
        appendPQExpBuffer(query, "CREATE %sSEQUENCE %s\n",
                         tbinfo->relpersistence == RELPERSISTENCE_UNLOGGED ? "UNLOGGED " : "",
                         fmtQualifiedDumpable(tbinfo));

        if (strcmp(seqtype, "bigint") != 0)
            appendPQExpBuffer(query, "    AS %s\n", seqtype);
    }

    // Add sequence parameters
    appendPQExpBuffer(query, "    START WITH %s\n", startv);
    appendPQExpBuffer(query, "    INCREMENT BY %s\n", incby);

    if (minv)
        appendPQExpBuffer(query, "    MINVALUE %s\n", minv);
    else
        appendPQExpBufferStr(query, "    NO MINVALUE\n");

    if (maxv)
        appendPQExpBuffer(query, "    MAXVALUE %s\n", maxv);
    else
        appendPQExpBufferStr(query, "    NO MAXVALUE\n");

    appendPQExpBuffer(query, "    CACHE %s%s", cache, (cycled ? "\n    CYCLE" : ""));

    if (tbinfo->is_identity_sequence)
        appendPQExpBufferStr(query, "\n);\n");
    else
        appendPQExpBufferStr(query, ";\n");

    // Create archive entry
    if (tbinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
        ArchiveEntry(fout, tbinfo->dobj.catId, tbinfo->dobj.dumpId,
                   ARCHIVE_OPTS(.tag = tbinfo->dobj.name,
                               .namespace = tbinfo->dobj.namespace->dobj.name,
                               .owner = tbinfo->rolname,
                               .description = "SEQUENCE",
                               .section = SECTION_PRE_DATA,
                               .createStmt = query->data,
                               .dropStmt = delqry->data));

    // Handle sequence ownership for non-identity sequences
    if (OidIsValid(tbinfo->owning_tab) && !tbinfo->is_identity_sequence) {
        owning_tab = findTableByOid(tbinfo->owning_tab);
        if (owning_tab && (owning_tab->dobj.dump & DUMP_COMPONENT_DEFINITION)) {
            resetPQExpBuffer(query);
            appendPQExpBuffer(query, "ALTER SEQUENCE %s", fmtQualifiedDumpable(tbinfo));
            appendPQExpBuffer(query, " OWNED BY %s", fmtQualifiedDumpable(owning_tab));
            appendPQExpBuffer(query, ".%s;\n", fmtId(owning_tab->attnames[tbinfo->owning_col - 1]));

            if (tbinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
                ArchiveEntry(fout, nilCatalogId, createDumpId(),
                           ARCHIVE_OPTS(.tag = tbinfo->dobj.name,
                                       .description = "SEQUENCE OWNED BY",
                                       .createStmt = query->data));
        }
    }

    // Dump comments and security labels
    if (tbinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpComment(fout, "SEQUENCE", qseqname, tbinfo->dobj.namespace->dobj.name,
                   tbinfo->rolname, tbinfo->dobj.catId, 0, tbinfo->dobj.dumpId);

    if (tbinfo->dobj.dump & DUMP_COMPONENT_SECLABEL)
        dumpSecLabel(fout, "SEQUENCE", qseqname, tbinfo->dobj.namespace->dobj.name,
                    tbinfo->rolname, tbinfo->dobj.catId, 0, tbinfo->dobj.dumpId);

    PQclear(res);
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(delqry);
    free(qseqname);
}
```