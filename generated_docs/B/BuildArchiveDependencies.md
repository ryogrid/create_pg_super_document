# BuildArchiveDependencies

## Location
src/bin/pg_dump/pg_dump.c: 18838 - 18885

## Overview
BuildArchiveDependencies creates dependency data for archive TOC entries by adjusting raw dependency data to handle dependency chains that link through objects not explicitly appearing in the dump.

## Definition


## Detailed Description
This function processes TOC entries in an archive dump to build correct dependencies for each entry. The raw dependency data from getDependencies() is not directly useful in archive dumps because dependency chains often link through objects that don't appear explicitly in the dump (like views depending on _RETURN rules). The function recursively searches DumpableObject data structures to build correct dependencies while preserving "special" dependencies (like TABLE DATA depending on its TABLE) that must remain as-is for pg_restore compatibility.

## Parameters / Member Variables
- : Archive pointer representing the dump archive being processed

## Dependencies
- Functions called/Symbols referenced:
  - findObjectByDumpId
  - findDumpableDependencies
  - pg_malloc
  - pg_realloc
- Types used:
  - TocEntry
  - DumpableObject
  - DumpId
- Called from (representative examples):
  - main (in pg_dump.c:1092)

## Notes and Other Information
- Only processes TOC entries that will be dumped (te->reqs > 0)
- Skips entries that already have "special" dependencies (te->nDeps > 0)
- Uses a work array that starts at 64 elements and grows as needed
- Preserves special dependencies created during ArchiveEntry calls
- Critical for ensuring proper restore order in pg_restore operations