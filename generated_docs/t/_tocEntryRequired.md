# _tocEntryRequired

## Location
[src/bin/pg_dump/pg_backup_archiver.c:2926-3206](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L2926-L3206)

## Overview
This function determines whether a table of contents entry should be restored during a PostgreSQL restore operation, applying various filtering rules and returning flags indicating which components (schema, data, or special) should be processed.

## Definition

```c
static int
_tocEntryRequired(TocEntry *te, teSection curSection, ArchiveHandle *AH)
```
## Detailed Description
The  function is the central decision-making component for selective restore operations in pg_restore. It evaluates a TOC entry against numerous criteria including restore options, object types, selective filters, and dependency relationships to determine what should be restored.

The function returns a combination of bit flags (REQ_SCHEMA, REQ_DATA, REQ_SPECIAL) or 0 to skip the entry entirely. It handles special cases for database objects, applies exclusion rules for ACLs/comments/security labels, enforces section-based filtering (pre-data/data/post-data), and implements selective restore logic for specific object types.

The decision logic flows through multiple stages: special entry handling, database creation rules, exclusion filters, section validation, ID-based filtering, and finally selective restore rules. For dependent objects like ACLs and comments, it checks whether their parent objects are being restored.

## Parameters / Member Variables
- `*te`: Table of contents entry being evaluated for restoration
- `curSection`: Current section being processed (pre-data, data, or post-data)
- `*AH`: Archive handle containing restoration context and options
## Dependencies
- Functions called/Symbols referenced:
  - strcmp/strncmp (C standard library string comparison)
  - [_tocEntryIsACL](_tocEntryIsACL.md) (helper function to identify ACL entries)
  - [getTocEntryByDumpId](../g/getTocEntryByDumpId.md) (lookup function for dependency resolution)
  - [simple_string_list_member](../s/simple_string_list_member.md) (utility for checking list membership)
  - REQ_SCHEMA, REQ_DATA, REQ_SPECIAL (bit flag constants)
  - [RestoreOptions](../R/RestoreOptions.md), TocEntry, teSection (struct types)
- Called from (representative examples):
  - [ProcessArchiveRestoreOptions](../P/ProcessArchiveRestoreOptions.md) (during restore option processing)
  - [PrintTOCSummary](../P/PrintTOCSummary.md) (when generating restore summaries)

## Notes and Other Information
- Returns combination of REQ_SCHEMA (1), REQ_DATA (2), and REQ_SPECIAL (4) bits
- Special entries (ENCODING, STDSTRINGS, SEARCHPATH) are always marked REQ_SPECIAL
- DATABASE entries are only restored when createDB option is enabled
- Implements complex dependency checking for ACLs, comments, and security labels
- Supports selective restoration by schema, table, index, function, and trigger names
- Handles exclusion lists and various skip options (ACLs, comments, publications, etc.)
- Manages schema-only and data-only restore modes with special cases for sequences and large objects
- Large object handling includes special rules for binary upgrade mode
- The function is critical for implementing pg_restore's flexible selective restore capabilities

## Simplified Source

```c
static int _tocEntryRequired(TocEntry *te, teSection curSection, ArchiveHandle *AH) {
    int res = REQ_SCHEMA | REQ_DATA;
    RestoreOptions *ropt = AH->public.ropt;

    // Handle special configuration entries
    if (strcmp(te->desc, "ENCODING") == 0 ||
        strcmp(te->desc, "STDSTRINGS") == 0 ||
        strcmp(te->desc, "SEARCHPATH") == 0)
        return REQ_SPECIAL;

    // DATABASE entries only restored if createDB is enabled
    if (strcmp(te->desc, "DATABASE") == 0 ||
        strcmp(te->desc, "DATABASE PROPERTIES") == 0)
        return ropt->createDB ? REQ_SCHEMA : 0;

    // Apply exclusion filters
    if (ropt->aclsSkip && _tocEntryIsACL(te)) return 0;
    if (ropt->no_comments && strcmp(te->desc, "COMMENT") == 0) return 0;
    if (ropt->no_publications &&
        (strcmp(te->desc, "PUBLICATION") == 0 ||
         strcmp(te->desc, "PUBLICATION TABLE") == 0)) return 0;

    // Check section filtering (pre-data/data/post-data)
    switch (curSection) {
        case SECTION_PRE_DATA:
            if (!(ropt->dumpSections & DUMP_PRE_DATA)) return 0;
            break;
        case SECTION_DATA:
            if (!(ropt->dumpSections & DUMP_DATA)) return 0;
            break;
        case SECTION_POST_DATA:
            if (!(ropt->dumpSections & DUMP_POST_DATA)) return 0;
            break;
        default:
            return 0;
    }

    // Apply ID-based filtering
    if (ropt->idWanted && !ropt->idWanted[te->dumpId - 1])
        return 0;

    // Handle dependent objects (ACL, COMMENT, SECURITY LABEL)
    if (strcmp(te->desc, "ACL") == 0 ||
        strcmp(te->desc, "COMMENT") == 0 ||
        strcmp(te->desc, "SECURITY LABEL") == 0) {

        // Database properties use createDB rule
        if (strncmp(te->tag, "DATABASE ", 9) == 0) {
            if (!ropt->createDB) return 0;
        }
        // Check if parent object is being restored
        else if (ropt->schemaNames.head != NULL || ropt->selTypes) {
            bool dumpthis = false;
            for (int i = 0; i < te->nDeps; i++) {
                TocEntry *pte = getTocEntryByDumpId(AH, te->dependencies[i]);
                if (pte && strcmp(pte->desc, "ACL") != 0 && pte->reqs != 0) {
                    dumpthis = true;
                    break;
                }
            }
            if (!dumpthis) return 0;
        }
    } else {
        // Apply selective restore rules for standalone objects
        if (ropt->schemaNames.head != NULL) {
            if (!te->namespace ||
                !simple_string_list_member(&ropt->schemaNames, te->namespace))
                return 0;
        }

        if (ropt->selTypes) {
            // Check object type filters (tables, indexes, functions, triggers)
            if (strcmp(te->desc, "TABLE") == 0 || strcmp(te->desc, "TABLE DATA") == 0) {
                if (!ropt->selTable ||
                    (ropt->tableNames.head &&
                     !simple_string_list_member(&ropt->tableNames, te->tag)))
                    return 0;
            } else if (strcmp(te->desc, "INDEX") == 0) {
                if (!ropt->selIndex ||
                    (ropt->indexNames.head &&
                     !simple_string_list_member(&ropt->indexNames, te->tag)))
                    return 0;
            }
            // Similar checks for functions, triggers...
        }
    }

    // Determine schema vs data components
    if (!te->hadDumper) {
        // Special data-only entries
        if (strcmp(te->desc, "SEQUENCE SET") == 0 ||
            strncmp(te->desc, "BLOB", 4) == 0)
            res = res & REQ_DATA;
        else
            res = res & ~REQ_DATA;
    }

    // Remove schema component if no definition
    if (!te->defn || !te->defn[0])
        res = res & ~REQ_SCHEMA;

    // Apply schema-only or data-only filtering
    if (ropt->schemaOnly) {
        if (!(ropt->sequence_data && strcmp(te->desc, "SEQUENCE SET") == 0))
            res = res & REQ_SCHEMA;
    }
    if (ropt->dataOnly)
        res = res & REQ_DATA;

    return res;
}
```