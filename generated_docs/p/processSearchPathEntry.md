# processSearchPathEntry

## Location
[src/bin/pg_dump/pg_backup_archiver.c:2866-2875](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L2866-L2875)

## Overview
This function processes a search_path configuration entry from a PostgreSQL archive during restoration, storing the path setting for later use during the restoration process.

## Definition

```c
static void
processSearchPathEntry(ArchiveHandle *AH, TocEntry *te)
```
## Detailed Description
The  function handles search_path configuration entries found in a PostgreSQL archive's table of contents. Unlike other configuration processors that parse and validate their input, this function simply copies the entire definition verbatim from the TOC entry to the archive handle's searchpath field.

The search_path setting controls which schemas are searched when resolving unqualified object names. By preserving this setting from the original database, the restoration process can maintain the same schema resolution behavior that existed when the dump was created.

## Parameters / Member Variables
- `*AH`: Archive handle containing the restoration context and configuration
- `*te`: Table of contents entry containing the search_path definition command
## Dependencies
- Functions called/Symbols referenced:
  - [pg_strdup](pg_strdup.md) (PostgreSQL string duplication utility)
  - [TocEntry](../T/TocEntry.md) (struct type)
- Called from (representative examples):
  - [ReadToc](../R/ReadToc.md) (during archive reading process)

## Notes and Other Information
- The function performs no validation or parsing of the search_path command
- The entire definition string is copied verbatim using pg_strdup for later execution
- The stored search_path will be applied during the restoration process to ensure proper schema resolution
- This approach allows for flexibility in search_path formats and future compatibility
- The copied string becomes the responsibility of the archive handle and will be freed when the handle is destroyed

## Simplified Source

```c
static void processSearchPathEntry(ArchiveHandle *AH, TocEntry *te) {
    // Store search_path command verbatim for later use during restoration
    AH->public.searchpath = pg_strdup(te->defn);
}
```