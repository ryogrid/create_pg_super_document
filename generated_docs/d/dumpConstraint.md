# dumpConstraint

## Location
[src/bin/pg_dump/pg_dump.c:17237-17548](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L17237-L17548)

## Overview
Writes out user-defined constraints to the dump archive, handling multiple constraint types including primary keys, unique constraints, foreign keys, and check constraints on both tables and domains.

## Definition

```c
static void
dumpConstraint(Archive *fout, const ConstraintInfo *coninfo)
```
## Detailed Description
The  function is a comprehensive constraint dumping handler that generates appropriate SQL statements for different constraint types in PostgreSQL. It handles the complexity of constraint restoration by generating both creation and deletion statements with proper dependencies and metadata.

The function processes several constraint types:

1. **Primary Key and Unique Constraints ('p', 'u', 'x')**: 
   - Generates ALTER TABLE ADD CONSTRAINT statements
   - Handles NULLS NOT DISTINCT behavior
   - Includes INCLUDE columns for covering indexes
   - Processes storage options and deferrability settings
   - Manages clustering and replica identity settings

2. **Foreign Key Constraints ('f')**:
   - Creates ALTER TABLE ADD CONSTRAINT FOREIGN KEY statements
   - Handles partitioned tables vs regular tables differently (ONLY clause)
   - Uses pre-computed constraint definitions from pg_get_constraintdef

3. **Check Constraints ('c')**:
   - On tables: Creates ALTER TABLE ADD CONSTRAINT CHECK statements
   - On domains: Creates ALTER DOMAIN ADD CONSTRAINT CHECK statements
   - Only processes local, non-inherited constraints when dumping separately

4. **Not Null Constraints ('n')**:
   - On domains: Creates ALTER DOMAIN ADD CONSTRAINT statements

The function also handles special cases like binary upgrades, foreign tables, partitioned tables, and extension dependencies.

## Parameters / Member Variables
- `*fout`: Archive pointer containing dump options and output context
- `*coninfo`: ConstraintInfo structure containing:
## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [findObjectByDumpId](../f/findObjectByDumpId.md)
  - [binary_upgrade_set_pg_class_oids](../b/binary_upgrade_set_pg_class_oids.md)
  - fmtQualifiedDumpable
  - [fmtId](../f/fmtId.md)
  - [getAttrName](../g/getAttrName.md)
  - [nonemptyReloptions](../n/nonemptyReloptions.md)
  - [appendReloptionsArrayAH](../a/appendReloptionsArrayAH.md)
  - [append_depends_on_extension](../a/append_depends_on_extension.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpTableConstraintComment](dumpTableConstraintComment.md)
  - [dumpComment](dumpComment.md)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)

## Notes and Other Information
- Skips processing in data-only dump mode as constraints are schema objects
- Index-backed constraints (PK/UNIQUE) require careful coordination with associated indexes
- Foreign key constraints on partitioned tables don't use ONLY keyword (inherit to partitions)
- Check constraints are only dumped if they're marked as 'separate' and 'local' (not inherited)
- Domain constraints get special comment handling with qualified object names
- Binary upgrade mode requires special handling for object OID preservation
- Keeps synchronization with dumpIndex for shared index properties like clustering and replica identity
- All constraints are dumped in SECTION_POST_DATA to ensure proper restoration order

## Simplified Source

```c
static void
dumpConstraint(Archive *fout, const ConstraintInfo *coninfo)
{
    DumpOptions *dopt = fout->dopt;
    TableInfo *tbinfo = coninfo->contable;
    PQExpBuffer q, delq;
    char *tag = NULL;
    char *foreign;

    // Skip in data-only mode
    if (dopt->dataOnly)
        return;

    q = createPQExpBuffer();
    delq = createPQExpBuffer();

    foreign = tbinfo && tbinfo->relkind == RELKIND_FOREIGN_TABLE ? "FOREIGN " : "";

    if (coninfo->contype == 'p' || coninfo->contype == 'u' || coninfo->contype == 'x')
    {
        // Handle primary key, unique, and exclusion constraints
        IndxInfo *indxinfo = (IndxInfo *) findObjectByDumpId(coninfo->conindex);

        if (indxinfo == NULL)
            pg_fatal("missing index for constraint \"%s\"", coninfo->dobj.name);

        // Generate ALTER TABLE ADD CONSTRAINT statement
        appendPQExpBuffer(q, "ALTER %sTABLE ONLY %s\n", foreign, fmtQualifiedDumpable(tbinfo));
        appendPQExpBuffer(q, "    ADD CONSTRAINT %s ", fmtId(coninfo->dobj.name));

        if (coninfo->condef) {
            // Use pre-computed constraint definition
            appendPQExpBuffer(q, "%s;\n", coninfo->condef);
        } else {
            // Build constraint definition manually
            appendPQExpBufferStr(q, coninfo->contype == 'p' ? "PRIMARY KEY" : "UNIQUE");

            // Add column list and options
            appendPQExpBufferStr(q, " (");
            for (int k = 0; k < indxinfo->indnkeyattrs; k++) {
                int indkey = (int) indxinfo->indkeys[k];
                if (indkey == InvalidAttrNumber) break;
                const char *attname = getAttrName(indkey, tbinfo);
                appendPQExpBuffer(q, "%s%s", (k == 0) ? "" : ", ", fmtId(attname));
            }
            appendPQExpBufferChar(q, ')');

            // Add storage options if present
            if (nonemptyReloptions(indxinfo->indreloptions)) {
                appendPQExpBufferStr(q, " WITH (");
                appendReloptionsArrayAH(q, indxinfo->indreloptions, "", fout);
                appendPQExpBufferChar(q, ')');
            }

            // Handle deferrable constraints
            if (coninfo->condeferrable) {
                appendPQExpBufferStr(q, " DEFERRABLE");
                if (coninfo->condeferred)
                    appendPQExpBufferStr(q, " INITIALLY DEFERRED");
            }
            appendPQExpBufferStr(q, ";\n");
        }

        // Handle clustering and replica identity settings
        if (indxinfo->indisclustered) {
            appendPQExpBuffer(q, "\nALTER TABLE %s CLUSTER ON %s;\n",
                            fmtQualifiedDumpable(tbinfo), fmtId(indxinfo->dobj.name));
        }

        if (indxinfo->indisreplident) {
            appendPQExpBuffer(q, "\nALTER TABLE ONLY %s REPLICA IDENTITY USING INDEX %s;\n",
                            fmtQualifiedDumpable(tbinfo), fmtId(indxinfo->dobj.name));
        }

        tag = psprintf("%s %s", tbinfo->dobj.name, coninfo->dobj.name);
    }
    else if (coninfo->contype == 'f')
    {
        // Handle foreign key constraints
        char *only = tbinfo->relkind == RELKIND_PARTITIONED_TABLE ? "" : "ONLY ";

        appendPQExpBuffer(q, "ALTER %sTABLE %s%s\n", foreign, only, fmtQualifiedDumpable(tbinfo));
        appendPQExpBuffer(q, "    ADD CONSTRAINT %s %s;\n", fmtId(coninfo->dobj.name), coninfo->condef);

        tag = psprintf("%s %s", tbinfo->dobj.name, coninfo->dobj.name);
    }
    else if (coninfo->contype == 'c' && tbinfo)
    {
        // Handle check constraints on tables
        if (coninfo->separate && coninfo->conislocal) {
            appendPQExpBuffer(q, "ALTER %sTABLE %s\n", foreign, fmtQualifiedDumpable(tbinfo));
            appendPQExpBuffer(q, "    ADD CONSTRAINT %s %s;\n", fmtId(coninfo->dobj.name), coninfo->condef);
            tag = psprintf("%s %s", tbinfo->dobj.name, coninfo->dobj.name);
        }
    }
    else if (tbinfo == NULL)
    {
        // Handle constraints on domains
        TypeInfo *tyinfo = coninfo->condomain;
        if (coninfo->separate) {
            const char *keyword = (coninfo->contype == 'c') ? "CHECK CONSTRAINT" : "CONSTRAINT";

            appendPQExpBuffer(q, "ALTER DOMAIN %s\n", fmtQualifiedDumpable(tyinfo));
            appendPQExpBuffer(q, "    ADD CONSTRAINT %s %s;\n", fmtId(coninfo->dobj.name), coninfo->condef);
            tag = psprintf("%s %s", tyinfo->dobj.name, coninfo->dobj.name);
        }
    }

    // Generate DROP statement
    if (coninfo->contype == 'p' || coninfo->contype == 'u' || coninfo->contype == 'x' ||
        coninfo->contype == 'f' || (coninfo->contype == 'c' && tbinfo)) {
        appendPQExpBuffer(delq, "ALTER %sTABLE %s DROP CONSTRAINT %s;\n",
                        foreign, fmtQualifiedDumpable(tbinfo), fmtId(coninfo->dobj.name));
    } else if (tbinfo == NULL) {
        TypeInfo *tyinfo = coninfo->condomain;
        appendPQExpBuffer(delq, "ALTER DOMAIN %s DROP CONSTRAINT %s;\n",
                        fmtQualifiedDumpable(tyinfo), fmtId(coninfo->dobj.name));
    }

    // Create archive entry if needed
    if (coninfo->dobj.dump & DUMP_COMPONENT_DEFINITION && tag) {
        ArchiveEntry(fout, coninfo->dobj.catId, coninfo->dobj.dumpId,
                   ARCHIVE_OPTS(.tag = tag,
                               .namespace = tbinfo ? tbinfo->dobj.namespace->dobj.name : coninfo->condomain->dobj.namespace->dobj.name,
                               .owner = tbinfo ? tbinfo->rolname : coninfo->condomain->rolname,
                               .description = "CONSTRAINT",
                               .section = SECTION_POST_DATA,
                               .createStmt = q->data,
                               .dropStmt = delq->data));
    }

    // Dump constraint comments
    if (tbinfo && coninfo->separate && (coninfo->dobj.dump & DUMP_COMPONENT_COMMENT))
        dumpTableConstraintComment(fout, coninfo);

    free(tag);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
}
```