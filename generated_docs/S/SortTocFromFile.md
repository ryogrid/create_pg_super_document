# SortTocFromFile

## Location
[src/bin/pg_dump/pg_backup_archiver.c:1548-1628](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L1548-L1628)

## Overview
Reads a TOC (Table of Contents) file to reorder archive entries according to a user-specified sequence and marks selected entries as wanted for restoration.

## Definition
```c
void SortTocFromFile(Archive *AHX)
```

## Detailed Description
This function processes a TOC file that contains a list of dump IDs specifying the desired restoration order. It reads the file line by line, validates each ID, finds the corresponding TOC entry, marks it as wanted, and moves it to the end of the TOC list to establish the specified order. Lines can contain comments (after ';') which are ignored. The function ensures that unwanted items remain at the front of the list, which is important for proper dependency handling in parallel restores.

## Parameters / Member Variables
- `AHX`: Archive handle cast from the generic Archive pointer

## Dependencies
- Functions called/Symbols referenced:
  - [RestoreOptions](../R/RestoreOptions.md)
  - [pg_malloc0](../p/pg_malloc0.md)
  - PG_BINARY_R
  - fopen
  - [pg_get_line_buf](../p/pg_get_line_buf.md)
  - DumpId
  - [TocEntry](../T/TocEntry.md)
  - pg_log_warning
  - [getTocEntryByDumpId](../g/getTocEntryByDumpId.md)
  - [_moveBefore](../m/_moveBefore.md)
  - [pg_free](../p/pg_free.md)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_restore.c)

## Notes and Other Information
- Allocates and initializes the idWanted boolean array based on maxDumpId
- Supports comments in TOC files (text after ';' is ignored)
- Validates dump IDs to ensure they are positive, within range, and not duplicated
- Moves selected entries to the end of the list in the order they appear in the file
- Unwanted items remain at the front, which helps with dependency resolution in parallel restores
- Uses pg_get_line_buf for robust line reading with proper memory management
- Provides warnings for invalid lines but continues processing
- Fatal errors occur for file I/O problems or missing TOC entries

## Simplified Source

```c
void SortTocFromFile(Archive *AHX)
{
    ArchiveHandle *AH = (ArchiveHandle *) AHX;
    RestoreOptions *ropt = AH->public.ropt;
    FILE *fh;
    StringInfoData linebuf;

    // Initialize idWanted array
    ropt->idWanted = (bool *) pg_malloc0(sizeof(bool) * AH->maxDumpId);

    // Open TOC file
    fh = fopen(ropt->tocFile, PG_BINARY_R);
    if (!fh)
        pg_fatal("could not open TOC file \"%s\": %m", ropt->tocFile);

    initStringInfo(&linebuf);

    // Process each line in the file
    while (pg_get_line_buf(fh, &linebuf)) {
        char *cmnt;
        char *endptr;
        DumpId id;
        TocEntry *te;

        // Remove comments (after ';')
        cmnt = strchr(linebuf.data, ';');
        if (cmnt != NULL) {
            cmnt[0] = '\0';
            linebuf.len = cmnt - linebuf.data;
        }

        // Skip blank lines
        if (strspn(linebuf.data, " \t\r\n") == linebuf.len)
            continue;

        // Parse and validate dump ID
        id = strtol(linebuf.data, &endptr, 10);
        if (endptr == linebuf.data || id <= 0 || id > AH->maxDumpId ||
            ropt->idWanted[id - 1]) {
            pg_log_warning("line ignored: %s", linebuf.data);
            continue;
        }

        // Find corresponding TOC entry
        te = getTocEntryByDumpId(AH, id);
        if (!te)
            pg_fatal("could not find entry for ID %d", id);

        // Mark as wanted and move to desired position
        ropt->idWanted[id - 1] = true;
        _moveBefore(AH->toc, te);
    }

    // Cleanup
    pg_free(linebuf.data);
    if (fclose(fh) != 0)
        pg_fatal("could not close TOC file: %m");
}
```