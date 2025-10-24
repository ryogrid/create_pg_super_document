# BuildArchiveDependencies

## Location
[src/bin/pg_dump/pg_dump.c:18838-18885](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L18838-L18885)

## Overview
BuildArchiveDependencies creates dependency data for archive TOC entries by adjusting raw dependency data to handle dependency chains that link through objects not explicitly appearing in the dump.

## Definition

```c
static void
BuildArchiveDependencies(Archive *fout)
```
## Detailed Description
This function processes TOC entries in an archive dump to build correct dependencies for each entry. The raw dependency data from getDependencies() is not directly useful in archive dumps because dependency chains often link through objects that don't appear explicitly in the dump (like views depending on _RETURN rules). The function recursively searches DumpableObject data structures to build correct dependencies while preserving "special" dependencies (like TABLE DATA depending on its TABLE) that must remain as-is for pg_restore compatibility.

## Parameters / Member Variables
- `*fout`: Archive pointer representing the dump archive being processed
## Dependencies
- Functions called/Symbols referenced:
  - [findObjectByDumpId](../f/findObjectByDumpId.md)
  - [findDumpableDependencies](../f/findDumpableDependencies.md)
  - [pg_malloc](../p/pg_malloc.md)
  - [pg_realloc](../p/pg_realloc.md)
- Types used:
  - [TocEntry](../T/TocEntry.md)
  - DumpableObject
  - DumpId
- Called from (representative examples):
  - [main](../m/main.md) (in pg_dump.c:1092)

## Notes and Other Information
- Only processes TOC entries that will be dumped (te->reqs > 0)
- Skips entries that already have "special" dependencies (te->nDeps > 0)
- Uses a work array that starts at 64 elements and grows as needed
- Preserves special dependencies created during ArchiveEntry calls
- Critical for ensuring proper restore order in pg_restore operations

## Simplified Source

```c
static void BuildArchiveDependencies(Archive *fout)
{
    ArchiveHandle *AH = (ArchiveHandle *) fout;

    // Process each TOC entry in the archive
    for (TocEntry *te = AH->toc->next; te != AH->toc; te = te->next) {
        // Skip entries that won't be dumped
        if (te->reqs == 0)
            continue;

        // Skip entries with existing "special" dependencies
        if (te->nDeps > 0)
            continue;

        // Find the original DumpableObject for this entry
        DumpableObject *dobj = findObjectByDumpId(te->dumpId);
        if (dobj == NULL || dobj->nDeps <= 0)
            continue;

        // Allocate dependency tracking array
        int allocDeps = 64;
        DumpId *dependencies = pg_malloc(allocDeps * sizeof(DumpId));
        int nDeps = 0;

        // Recursively find all dumpable dependencies
        findDumpableDependencies(AH, dobj, &dependencies, &nDeps, &allocDeps);

        // Save dependencies to TOC entry
        if (nDeps > 0) {
            dependencies = pg_realloc(dependencies, nDeps * sizeof(DumpId));
            te->dependencies = dependencies;
            te->nDeps = nDeps;
        } else {
            free(dependencies);
        }
    }
}
```