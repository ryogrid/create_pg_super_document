# ReadToc

## Location
[src/bin/pg_dump/pg_backup_archiver.c:2649-2820](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L2649-L2820)

## Overview
Reads and reconstructs the Table of Contents from an archive file, creating the in-memory TOC structure needed for restore operations.

## Definition
```c
void ReadToc(ArchiveHandle *AH)
```

## Detailed Description
This function deserializes the Table of Contents from an archive file and builds the complete in-memory representation of all database objects contained in the dump. It handles version compatibility by reading different fields based on the archive format version, ensuring that archives created by older versions of pg_dump can still be read. The function processes dependencies, handles backward compatibility for section classifications, and performs immediate processing for special entries like encoding settings. Each TOC entry is linked into a circular linked list for efficient traversal during restore operations.

## Parameters / Member Variables
- `AH`: Archive handle that will be populated with the TOC structure and metadata from the archive

## Dependencies
- Functions called/Symbols referenced:
  - [TocEntry](../T/TocEntry.md), DumpId (struct types)
  - [ReadInt](ReadInt.md), ReadStr (archive reading functions)
  - [pg_malloc0](../p/pg_malloc0.md), pg_malloc, pg_realloc (memory management)
  - pg_log_warning, pg_log_debug (logging functions)
  - [processEncodingEntry](../p/processEncodingEntry.md), processStdStringsEntry, processSearchPathEntry (special entry processors)
  - Version constants (K_VERS_1_3, K_VERS_1_5, etc.)
  - Section constants (SECTION_NONE, SECTION_DATA, SECTION_POST_DATA, SECTION_PRE_DATA)
- Called from (representative examples):
  - [InitArchiveFmt_Custom](../I/InitArchiveFmt_Custom.md)
  - [InitArchiveFmt_Directory](../I/InitArchiveFmt_Directory.md)

## Notes and Other Information
- Handles version compatibility across multiple archive format versions (1.3 through 1.16+)
- For pre-8.4 archives, manually classifies entries into sections based on description strings
- Dynamically allocates and resizes dependency arrays as needed
- Maintains maxDumpId to track the highest dump ID encountered
- Links all entries into a circular doubly-linked list for efficient navigation
- Performs sanity checking on dump IDs to detect corrupt archives
- Issues warnings for deprecated features like tables WITH OIDS
- Immediately processes special configuration entries (ENCODING, STDSTRINGS, SEARCHPATH)
- The function is critical for restore operations as it builds the complete object dependency graph
- Dependencies are stored as arrays of DumpId values, terminated by NULL in the serialized format
- Each entry includes comprehensive metadata needed for restoration: SQL statements, ownership, tablespace, etc.

## Simplified Source

```c
void ReadToc(ArchiveHandle *AH) {
    TocEntry *te;
    char *tmp;
    DumpId *deps;
    int depIdx, depSize;

    // Read total count of TOC entries
    AH->tocCount = ReadInt(AH);
    AH->maxDumpId = 0;

    for (int i = 0; i < AH->tocCount; i++) {
        // Create new TOC entry
        te = pg_malloc0(sizeof(TocEntry));
        te->dumpId = ReadInt(AH);

        if (te->dumpId > AH->maxDumpId)
            AH->maxDumpId = te->dumpId;

        // Read basic entry information
        te->hadDumper = ReadInt(AH);

        // Read catalog OID (version dependent)
        if (AH->version >= K_VERS_1_8) {
            tmp = ReadStr(AH);
            sscanf(tmp, "%u", &te->catalogId.tableoid);
            free(tmp);
        }

        tmp = ReadStr(AH);
        sscanf(tmp, "%u", &te->catalogId.oid);
        free(tmp);

        // Read descriptive information
        te->tag = ReadStr(AH);
        te->desc = ReadStr(AH);

        // Determine section (version dependent)
        if (AH->version >= K_VERS_1_11) {
            te->section = ReadInt(AH);
        } else {
            // Classify entries for older archives
            if (strcmp(te->desc, "TABLE DATA") == 0 ||
                strcmp(te->desc, "BLOBS") == 0)
                te->section = SECTION_DATA;
            else if (strcmp(te->desc, "CONSTRAINT") == 0 ||
                     strcmp(te->desc, "INDEX") == 0)
                te->section = SECTION_POST_DATA;
            else
                te->section = SECTION_PRE_DATA;
        }

        // Read SQL statements and metadata
        te->defn = ReadStr(AH);
        te->dropStmt = ReadStr(AH);
        if (AH->version >= K_VERS_1_3)
            te->copyStmt = ReadStr(AH);
        if (AH->version >= K_VERS_1_6)
            te->namespace = ReadStr(AH);

        te->owner = ReadStr(AH);

        // Read dependencies
        if (AH->version >= K_VERS_1_5) {
            depSize = 100;
            deps = pg_malloc(sizeof(DumpId) * depSize);
            depIdx = 0;

            while ((tmp = ReadStr(AH)) != NULL) {
                if (depIdx >= depSize) {
                    depSize *= 2;
                    deps = pg_realloc(deps, sizeof(DumpId) * depSize);
                }
                sscanf(tmp, "%d", &deps[depIdx++]);
                free(tmp);
            }

            te->dependencies = depIdx > 0 ?
                pg_realloc(deps, sizeof(DumpId) * depIdx) : NULL;
            te->nDeps = depIdx;
            if (depIdx == 0) free(deps);
        }

        // Link into circular list
        te->prev = AH->toc->prev;
        AH->toc->prev->next = te;
        AH->toc->prev = te;
        te->next = AH->toc;

        // Process special entries immediately
        if (strcmp(te->desc, "ENCODING") == 0)
            processEncodingEntry(AH, te);
        else if (strcmp(te->desc, "STDSTRINGS") == 0)
            processStdStringsEntry(AH, te);
        else if (strcmp(te->desc, "SEARCHPATH") == 0)
            processSearchPathEntry(AH, te);
    }
}
```