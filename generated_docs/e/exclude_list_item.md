# exclude_list_item

## Location
[src/bin/pg_rewind/filemap.c:101-195](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/filemap.c#L101-L195)

## Overview
The `exclude_list_item` structure defines elements for exclusion lists used in PostgreSQL's checksum validation and base backup operations to specify files or paths that should be excluded from processing.

## Definition
```c
struct exclude_list_item
{
    const char *name;
    bool        match_prefix;
};
```

## Detailed Description
This structure represents a single item in an exclusion list used by various PostgreSQL utilities including base backup operations, checksum validation (pg_checksums), and rewind operations (pg_rewind). The structure allows for two types of exclusion matching: exact name matching and prefix-based matching. When `match_prefix` is true, any file or directory whose path starts with the specified name will be excluded. This flexible design enables efficient exclusion of entire directory trees or specific files.

The structure is used across multiple PostgreSQL components to maintain consistent exclusion behavior, particularly for temporary files, system directories, and other paths that should not be included in backups or checksum calculations.

## Parameters / Member Variables
- `name`: A string containing the name of the file or path to check for exclusion
- `match_prefix`: A boolean flag indicating whether to match items using the name as a prefix (true) or requiring exact name matching (false)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_STAT_TMP_DIR` (used in exclusion lists)
  - `PG_DYNSHMEM_DIR` (used in exclusion lists)
  - `PG_AUTOCONF_FILENAME` (used in exclusion lists)
  - `LOG_METAINFO_DATAFILE_TMP` (used in exclusion lists)
  - `RELCACHE_INIT_FILENAME` (used in exclusion lists)
  - `BACKUP_LABEL_FILE` (used in exclusion lists)
  - `TABLESPACE_MAP` (used in exclusion lists)
- Called from (representative examples):
  - Used in base backup operations (src/backend/backup/basebackup.c)
  - Used in checksum validation (src/bin/pg_checksums/pg_checksums.c)
  - Used in rewind operations (src/bin/pg_rewind/filemap.c)

## Notes and Other Information
- This structure is used consistently across multiple PostgreSQL utilities to maintain synchronized exclusion behavior
- The design supports both exact matching and prefix-based exclusion for flexible file filtering
- Common usage includes excluding temporary directories, statistics files, and system-generated files that should not be part of backups
- The structure definition is located at src/backend/backup/basebackup.c:137-141
- Exclusion lists using this structure help ensure that backups and other operations exclude transient or system-specific files that could interfere with restoration or validation processes