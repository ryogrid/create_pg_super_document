# ArchiveEntry

## Location
[src/bin/pg_dump/pg_backup_archiver.c:1222-1280](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L1222-L1280)

## Overview
ArchiveEntry creates a new Table of Contents (TOC) entry that serves as the central metadata repository for all database objects during PostgreSQL dump operations, managing object information, dependencies, and dump context.

## Definition

```c
TocEntry *
ArchiveEntry(Archive *AHX, CatalogId catalogId, DumpId dumpId,
			 ArchiveOpts *opts)
```
## Detailed Description
ArchiveEntry is a fundamental function in the pg_dump archiver that creates and initializes new TOC entries for database objects being dumped. Despite its name suggesting a simple table of contents, this function actually creates comprehensive metadata records that serve as the primary repository for all information about database objects during the dump process.

The function performs several critical operations: it allocates memory for a new TocEntry structure, links it into the archive's doubly-linked TOC list, copies object metadata from the provided ArchiveOpts structure, manages object dependencies, and calls format-specific archive entry handlers. The TOC entry becomes the authoritative record for the object throughout the dump and restore process.

The function maintains archive statistics by incrementing the TOC count and tracking the maximum dump ID. It carefully manages memory allocation for variable-length fields like object names, statements, and dependency arrays. The resulting TOC entry contains all necessary information for both dumping the object's data and recreating the object during restore.

## Parameters / Member Variables
- `*AHX`: Archive pointer representing the current dump session context
- `catalogId`: PostgreSQL system catalog identifier for the database object
- `dumpId`: Unique identifier for this object within the dump session
- `*opts`: ArchiveOpts structure containing all metadata and options for the object being archived
## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc0](../p/pg_malloc0.md) (for TocEntry allocation)
  - [pg_malloc](../p/pg_malloc.md) (for dependencies array allocation)
  - [pg_strdup](../p/pg_strdup.md) (for string duplication)
  - memcpy (for copying dependency arrays)
  - AH->ArchiveEntryPtr (format-specific entry handler)
- Called from (representative examples):
  - [dumpTableData](../d/dumpTableData.md)
  - [dumpDatabase](../d/dumpDatabase.md)
  - [dumpNamespace](../d/dumpNamespace.md)
  - [dumpFunc](../d/dumpFunc.md)
  - [dumpIndex](../d/dumpIndex.md)
  - [dumpConstraint](../d/dumpConstraint.md)
  - [dumpSequence](../d/dumpSequence.md)

## Notes and Other Information
- The TOC was originally designed as a table of contents but has evolved into the complete metadata repository
- Each TOC entry is linked into a doubly-linked list for efficient traversal during restore operations
- The function handles optional fields gracefully, only allocating memory and copying data when present
- Dependencies between objects are tracked through the dependencies array, enabling proper restore ordering
- Format-specific handlers can perform additional processing on the TOC entry after creation
- The hadDumper flag tracks whether the object has associated data dumping functionality
- Memory management is carefully handled to prevent leaks while supporting variable-length object metadata

## Simplified Source

```c
TocEntry *
ArchiveEntry(Archive *AHX, CatalogId catalogId, DumpId dumpId,
             ArchiveOpts *opts)
{
    ArchiveHandle *AH = (ArchiveHandle *) AHX;
    TocEntry *newToc;

    // Allocate and initialize new TOC entry
    newToc = (TocEntry *) pg_malloc0(sizeof(TocEntry));

    // Update archive statistics
    AH->tocCount++;
    if (dumpId > AH->maxDumpId)
        AH->maxDumpId = dumpId;

    // Link entry into doubly-linked TOC list
    newToc->prev = AH->toc->prev;
    newToc->next = AH->toc;
    AH->toc->prev->next = newToc;
    AH->toc->prev = newToc;

    // Set basic identifiers and section
    newToc->catalogId = catalogId;
    newToc->dumpId = dumpId;
    newToc->section = opts->section;

    // Copy object metadata strings
    newToc->tag = pg_strdup(opts->tag);
    newToc->namespace = opts->namespace ? pg_strdup(opts->namespace) : NULL;
    newToc->tablespace = opts->tablespace ? pg_strdup(opts->tablespace) : NULL;
    newToc->tableam = opts->tableam ? pg_strdup(opts->tableam) : NULL;
    newToc->owner = opts->owner ? pg_strdup(opts->owner) : NULL;
    newToc->desc = pg_strdup(opts->description);
    newToc->defn = opts->createStmt ? pg_strdup(opts->createStmt) : NULL;
    newToc->dropStmt = opts->dropStmt ? pg_strdup(opts->dropStmt) : NULL;
    newToc->copyStmt = opts->copyStmt ? pg_strdup(opts->copyStmt) : NULL;

    // Copy object properties
    newToc->relkind = opts->relkind;

    // Handle dependencies array
    if (opts->nDeps > 0) {
        newToc->dependencies = (DumpId *) pg_malloc(opts->nDeps * sizeof(DumpId));
        memcpy(newToc->dependencies, opts->deps, opts->nDeps * sizeof(DumpId));
        newToc->nDeps = opts->nDeps;
    } else {
        newToc->dependencies = NULL;
        newToc->nDeps = 0;
    }

    // Set dumper function information
    newToc->dataDumper = opts->dumpFn;
    newToc->dataDumperArg = opts->dumpArg;
    newToc->hadDumper = opts->dumpFn ? true : false;

    // Initialize format data
    newToc->formatData = NULL;
    newToc->dataLength = 0;

    // Call format-specific entry handler if available
    if (AH->ArchiveEntryPtr != NULL)
        AH->ArchiveEntryPtr(AH, newToc);

    return newToc;
}
```