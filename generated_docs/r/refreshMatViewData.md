# refreshMatViewData

## Location
src/bin/pg_dump/pg_dump.c: 2771 - 2805

## Overview
Creates an archive entry containing a REFRESH MATERIALIZED VIEW statement to repopulate a materialized view during database restore.

## Definition
```c
static void refreshMatViewData(Archive *fout, const TableDataInfo *tdinfo)
```

## Detailed Description
This function generates an archive entry for refreshing materialized view data rather than directly executing the refresh. It constructs a REFRESH MATERIALIZED VIEW SQL statement and packages it as an archive entry that will be executed during the restore process. The function includes a safeguard to skip unpopulated materialized views (those created with NO DATA) since they don't require refreshing.

The generated archive entry is placed in the SECTION_POST_DATA section, ensuring that the materialized view refresh occurs after all base tables and their data have been restored, which is necessary since materialized views depend on the underlying table data.

## Parameters / Member Variables
- `fout`: Archive structure for writing dump entries and accessing dump options
- `tdinfo`: TableDataInfo structure containing metadata about the materialized view to be refreshed

## Dependencies
- Functions called/Symbols referenced:
  - [TableInfo](../T/TableInfo.md) (structure type)
  - fmtQualifiedDumpable
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - ARCHIVE_OPTS (macro)
  - createPQExpBuffer/destroyPQExpBuffer
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - DUMP_COMPONENT_DATA (flag)
  - SECTION_POST_DATA (section constant)
- Called from (representative examples):
  - fmtQualifiedDumpable (src/bin/pg_dump/pg_dump.c:207)
  - [dumpDumpableObject](../d/dumpDumpableObject.md) (src/bin/pg_dump/pg_dump.c:10592)

## Notes and Other Information
- This is a static function within pg_dump.c
- Only creates archive entries for populated materialized views (relispopulated = true)
- [Archive](../A/Archive.md) entry is placed in SECTION_POST_DATA to ensure proper restore ordering
- Essential for maintaining materialized view consistency during database restoration
- Complements regular table data dumping for materialized views
- Part of PostgreSQL's comprehensive backup and restore infrastructure
- Located at src/bin/pg_dump/pg_dump.c:2771-2805