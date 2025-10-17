# TocIDRequired

## Location
[src/bin/pg_dump/pg_backup_archiver.c:2020-2030](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L2020-L2030)

## Overview
Checks whether a TOC entry with the specified dump ID is required for the current restoration operation.

## Definition
```c
int TocIDRequired(ArchiveHandle *AH, DumpId id)
```

## Detailed Description
This function determines if a specific TOC entry (identified by dump ID) is required for the current dump or restore operation. It provides a simple interface to check the requirements status of any TOC entry without needing to handle the underlying TOC lookup logic.

The function works by:
1. Looking up the TOC entry using the provided dump ID via `getTocEntryByDumpId`
2. Returning the `reqs` field value if the entry exists
3. Returning 0 if the entry doesn't exist (indicating not required)

The `reqs` field typically contains flags indicating various requirements such as whether the entry should be included in the current operation, whether it has specific dependencies, or other operational constraints.

## Parameters / Member Variables
- `AH`: Archive handle containing the TOC entries and their requirement information
- `id`: The dump ID of the TOC entry to check

## Dependencies
- Functions called/Symbols referenced:
  - [getTocEntryByDumpId](../g/getTocEntryByDumpId.md) (TOC entry lookup)
  - DumpId (type)
  - [TocEntry](TocEntry.md) (struct type)
- Called from (representative examples):
  - [findDumpableDependencies](../f/findDumpableDependencies.md) (dependency analysis during dump creation)
  - [_tarPositionTo](../t/_tarPositionTo.md) (tar format archive positioning)

## Notes and Other Information
- Returns 0 for non-existent TOC entries, which is interpreted as "not required"
- The function provides a safe way to check requirements without direct TOC entry access
- Used primarily during dependency analysis and selective restoration operations
- The exact meaning of the returned requirement flags depends on the context of the archive operation
- This function is part of the public interface exposed through pg_backup_archiver.h
- Essential for determining which entries to include in partial dumps or selective restores

## Simplified Source

```c
int
TocIDRequired(ArchiveHandle *AH, DumpId id)
{
    TocEntry *te = getTocEntryByDumpId(AH, id);

    if (!te)
        return 0;

    return te->reqs;
}
```