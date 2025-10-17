# _printTocEntry

## Location
[src/bin/pg_dump/pg_backup_archiver.c:3758-3950](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L3758-L3950)

## Overview
Emits the SQL commands to create the object represented by a TOC entry, including header comments, object definition, and ALTER OWNER commands for pg_dump restoration operations.

## Definition

```c
struct tm	crtm;
```
## Detailed Description
This function is the core output generator for pg_dump's restore process. It handles the complete restoration workflow for database objects by:

1. **Setting context**: Selects appropriate owner, schema, tablespace, and access method
2. **Generating comments**: Creates descriptive header comments with object metadata and dependencies
3. **Processing definitions**: Handles three special cases:
   - Schema definitions with --no-owner mode (strips AUTHORIZATION clause)
   - BLOB METADATA entries (processes OID lists)
   - ACL LARGE OBJECTS entries (applies ACL commands to multiple objects)
4. **Owner restoration**: Issues ALTER OWNER commands when not using SET SESSION AUTH
5. **Post-processing**: Handles partitioned table access methods and ACL session cleanup

The function manages transaction counting for bulk operations by counting semicolons in SQL definitions (excluding functions/procedures) and handles various edge cases for different PostgreSQL object types.

## Parameters / Member Variables
- : ArchiveHandle pointer containing archive state and output functions
- : TocEntry pointer with object metadata (name, type, definition, owner, dependencies, etc.)
- : Boolean flag indicating whether this is a data entry (affects comment prefix)

## Dependencies
- Functions called/Symbols referenced:
  - [TocEntry](../T/TocEntry.md), RestoreOptions (struct types)
  - [_becomeOwner](../b/_becomeOwner.md), _selectOutputSchema, _selectTablespace, _selectTableAccessMethod
  - [ahprintf](../a/ahprintf.md) (formatted output to archive)
  - [sanitize_line](../s/sanitize_line.md) (comment sanitization)
  - [fmtId](../f/fmtId.md) (identifier quoting)
  - [IssueCommandPerBlob](../I/IssueCommandPerBlob.md), IssueACLPerBlob (special BLOB handling)
  - [_getObjectDescription](../g/_getObjectDescription.md) (object description generation)
  - [initPQExpBuffer](../i/initPQExpBuffer.md), termPQExpBuffer (buffer management)
  - [_printTableAccessMethodNoStorage](_printTableAccessMethodNoStorage.md) (partitioned table handling)
  - [_tocEntryIsACL](../t/_tocEntryIsACL.md) (ACL entry detection)
- Called from:
  - [restore_toc_entry](../r/restore_toc_entry.md) (main restoration function, multiple call sites)

## Notes and Other Information
- Function is static and only used within pg_backup_archiver.c
- Handles complex restore scenarios including no-owner mode, blob metadata, and ACL processing
- Uses transaction counting heuristics for bulk operations (counting semicolons)
- Special handling for schema "public" when using comment-based creation
- Manages session authorization state cleanup after ACL processing
- Supports both verbose and compact output modes through AH->public.verbose
- Integrates with tablespace and access method selection for proper object placement

## Simplified Source

```c
static void
_printTocEntry(ArchiveHandle *AH, TocEntry *te, bool isData)
{
    RestoreOptions *ropt = AH->public.ropt;

    // Set context: owner, schema, tablespace, access method
    _becomeOwner(AH, te);
    _selectOutputSchema(AH, te->namespace);
    _selectTablespace(AH, te->tablespace);
    if (te->relkind != RELKIND_PARTITIONED_TABLE)
        _selectTableAccessMethod(AH, te->tableam);

    // Generate header comments if enabled
    if (!AH->noTocComments)
    {
        const char *prefix = isData ? "Data for " : "";

        // Output TOC entry info and dependencies
        ahprintf(AH, "--\n");
        if (AH->public.verbose)
        {
            ahprintf(AH, "-- TOC entry %d (class %u OID %u)\n",
                     te->dumpId, te->catalogId.tableoid, te->catalogId.oid);
            // Print dependencies if any
            if (te->nDeps > 0)
            {
                ahprintf(AH, "-- Dependencies:");
                for (int i = 0; i < te->nDeps; i++)
                    ahprintf(AH, " %d", te->dependencies[i]);
                ahprintf(AH, "\n");
            }
        }

        // Format object description with sanitized names
        char *sanitized_name = sanitize_line(te->tag, false);
        char *sanitized_schema = sanitize_line(te->namespace, true);
        char *sanitized_owner = sanitize_line(ropt->noOwner ? NULL : te->owner, true);

        ahprintf(AH, "-- %sName: %s; Type: %s; Schema: %s; Owner: %s",
                 prefix, sanitized_name, te->desc, sanitized_schema, sanitized_owner);

        // Add tablespace info if present
        if (te->tablespace && strlen(te->tablespace) > 0 && !ropt->noTablespace)
        {
            char *sanitized_tablespace = sanitize_line(te->tablespace, false);
            ahprintf(AH, "; Tablespace: %s", sanitized_tablespace);
            free(sanitized_tablespace);
        }
        ahprintf(AH, "\n");

        // Call extra TOC printer if available
        if (AH->PrintExtraTocPtr != NULL)
            AH->PrintExtraTocPtr(AH, te);
        ahprintf(AH, "--\n\n");

        // Cleanup
        free(sanitized_name);
        free(sanitized_schema);
        free(sanitized_owner);
    }

    // Process object definition based on type
    if (ropt->noOwner && strcmp(te->desc, "SCHEMA") == 0 && strncmp(te->defn, "--", 2) != 0)
    {
        // Special case: CREATE SCHEMA without AUTHORIZATION
        ahprintf(AH, "CREATE SCHEMA %s;\n\n\n", fmtId(te->tag));
    }
    else if (strcmp(te->desc, "BLOB METADATA") == 0)
    {
        // Special case: BLOB creation
        IssueCommandPerBlob(AH, te, "SELECT pg_catalog.lo_create('", "')");
    }
    else if (strcmp(te->desc, "ACL") == 0 && strncmp(te->tag, "LARGE OBJECTS", 13) == 0)
    {
        // Special case: ACL for large objects
        IssueACLPerBlob(AH, te);
    }
    else if (te->defn && strlen(te->defn) > 0)
    {
        // Normal case: output definition
        ahprintf(AH, "%s\n\n", te->defn);

        // Count transactions for bulk operations (excluding functions)
        if (ropt->txn_size > 0 &&
            strcmp(te->desc, "FUNCTION") != 0 &&
            strcmp(te->desc, "PROCEDURE") != 0)
        {
            // Count semicolons as transaction estimates
            const char *p = te->defn;
            int semicolon_count = 0;
            while ((p = strchr(p, ';')) != NULL)
            {
                semicolon_count++;
                p++;
            }
            if (semicolon_count > 1)
                AH->txnCount += semicolon_count - 1;
        }
    }

    // Issue ALTER OWNER command if needed
    if (!ropt->noOwner &&
        (!ropt->use_setsessauth ||
         (strcmp(te->desc, "SCHEMA") == 0 && strncmp(te->defn, "--", 2) == 0)) &&
        te->owner && strlen(te->owner) > 0 &&
        te->dropStmt && strlen(te->dropStmt) > 0)
    {
        if (strcmp(te->desc, "BLOB METADATA") == 0)
        {
            // BLOB ownership change
            char *owner_cmd = psprintf(" OWNER TO %s", fmtId(te->owner));
            IssueCommandPerBlob(AH, te, "ALTER LARGE OBJECT ", owner_cmd);
            pg_free(owner_cmd);
        }
        else
        {
            // Regular object ownership change
            PQExpBufferData description;
            initPQExpBuffer(&description);
            _getObjectDescription(&description, te);

            if (description.data[0])
                ahprintf(AH, "ALTER %s OWNER TO %s;\n\n",
                         description.data, fmtId(te->owner));
            termPQExpBuffer(&description);
        }
    }

    // Handle partitioned table access method
    if (te->relkind == RELKIND_PARTITIONED_TABLE)
        _printTableAccessMethodNoStorage(AH, te);

    // Clear current user if processing ACL (may change session auth)
    if (_tocEntryIsACL(te))
    {
        free(AH->currUser);
        AH->currUser = NULL;
    }
}
```