# identify_locking_dependencies

## Location
[src/bin/pg_dump/pg_backup_archiver.c:4839-4902](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L4839-L4902)

## Overview
This function identifies database objects that require exclusive locks during parallel restore operations, specifically for POST_DATA section items, and records their dump IDs in the entry's lockDeps array.

## Definition
static void identify_locking_dependencies(ArchiveHandle *AH, TocEntry *te)

## Detailed Description
The function analyzes a given TOC (Table of Contents) entry to determine which database objects will need exclusive locks when restoring that entry during a parallel restore operation. It focuses exclusively on POST_DATA items since PRE_DATA items are not run in parallel and DATA items are assumed to be independent. The function examines the entry's dependencies and identifies TABLE or TABLE DATA items that will require exclusive locking, storing their dump IDs in the lockDeps array.

Most POST_DATA items are ALTER TABLE operations or equivalent commands that require exclusive table locks. However, the function makes an exception for CREATE INDEX operations, which do not require exclusive locks. The function accounts for dependency repointing that may have occurred (via repoint_table_dependencies), where original TABLE dependencies might have been redirected to TABLE DATA dependencies.

## Parameters / Member Variables
- : Archive handle containing the dump metadata and TOC entries
- : The specific TOC entry to analyze for locking dependencies

## Dependencies
- Functions called/Symbols referenced:
  - [TocEntry](../T/TocEntry.md) (struct type)
  - DumpId (type)
  - SECTION_POST_DATA (constant)
  - [pg_malloc](../p/pg_malloc.md) (memory allocation function)
  - [pg_realloc](../p/pg_realloc.md) (memory reallocation function)
  - strcmp (string comparison function)
  - free (memory deallocation function)
- Called from (representative examples):
  - [fix_dependencies](../f/fix_dependencies.md)

## Notes and Other Information
- Only processes POST_DATA section entries; other sections are ignored
- CREATE INDEX operations are exempt from lock dependency analysis
- The function handles both TABLE and TABLE DATA dependencies, accounting for potential repointing
- Memory is allocated dynamically for the lockDeps array and resized to fit the actual number of locking dependencies
- Assumes that TABLE and TABLE DATA dependencies in POST_DATA context require exclusive locks
- Essential for preventing deadlocks and ensuring proper sequencing in parallel restore operations

## Simplified Source

```c
static void identify_locking_dependencies(ArchiveHandle *AH, TocEntry *te) {
    DumpId *lockids;
    int nlockids;
    int i;

    // Only process POST_DATA items for parallel execution
    if (te->section != SECTION_POST_DATA)
        return;

    // Skip if no dependencies
    if (te->nDeps == 0)
        return;

    // CREATE INDEX doesn't need exclusive locks
    if (strcmp(te->desc, "INDEX") == 0)
        return;

    // Allocate array for lock dependencies
    lockids = (DumpId *) pg_malloc(te->nDeps * sizeof(DumpId));
    nlockids = 0;

    // Find TABLE or TABLE DATA dependencies that need locks
    for (i = 0; i < te->nDeps; i++) {
        DumpId depid = te->dependencies[i];

        if (depid <= AH->maxDumpId && AH->tocsByDumpId[depid] != NULL &&
            ((strcmp(AH->tocsByDumpId[depid]->desc, "TABLE DATA") == 0) ||
             strcmp(AH->tocsByDumpId[depid]->desc, "TABLE") == 0)))
            lockids[nlockids++] = depid;
    }

    // Store results or cleanup
    if (nlockids == 0) {
        free(lockids);
        return;
    }

    te->lockDeps = pg_realloc(lockids, nlockids * sizeof(DumpId));
    te->nLockDeps = nlockids;
}
```