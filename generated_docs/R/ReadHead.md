# ReadHead

## Location
[src/bin/pg_dump/pg_backup_archiver.c:3977-4110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L3977-L4110)

## Overview
Reads and validates the file header from a PostgreSQL custom-format archive, extracting version information, compression settings, timestamps, and metadata for proper archive processing.

## Definition

```c
struct tm	crtm;
```
## Detailed Description
This function is the counterpart to WriteHead, responsible for parsing and validating archive headers during restore operations. It performs comprehensive validation and extraction:

1. **Magic number validation**: Verifies "PGDMP" signature (if not already read)
2. **Version compatibility**: Checks archive format version against supported range (K_VERS_1_0 to K_VERS_MAX)
3. **Architecture compatibility**: Validates integer and offset sizes, warns about potential cross-platform issues
4. **Format verification**: Ensures expected archive format matches actual format
5. **Compression handling**: Determines compression algorithm based on version, with backward compatibility for older formats
6. **Timestamp reconstruction**: Reconstructs creation date with timezone handling workarounds
7. **Metadata extraction**: Reads database name and version information when available

The function handles version evolution gracefully, supporting multiple archive format versions with appropriate fallbacks and compatibility checks.

## Parameters / Member Variables
- : ArchiveHandle pointer that gets populated with archive metadata including version, compression settings, creation date, database name, and version information

## Dependencies
- Functions called/Symbols referenced:
  - MAKE_ARCHIVE_VERSION (version encoding macro)
  - K_VERS_1_0, K_VERS_MAX, K_VERS_1_2, K_VERS_1_4, K_VERS_1_7, K_VERS_1_10, K_VERS_1_15 (version constants)
  - ReadBufPtr, ReadBytePtr, ReadInt, ReadStr (binary reading functions)
  - PG_COMPRESSION_GZIP (compression constant)
  - [supports_compression](../s/supports_compression.md) (compression support validation)
  - [pg_fatal](../p/pg_fatal.md), pg_log_warning, pg_free (error handling and logging)
  - strncmp, mktime (standard C library functions)
  - struct tm (time structure)
- Called from:
  - [InitArchiveFmt_Custom](../I/InitArchiveFmt_Custom.md), InitArchiveFmt_Directory (archive format initialization)

## Notes and Other Information
- Function is non-static and used by multiple archive format implementations
- Includes comprehensive error checking for corrupted or incompatible archives
- Handles timezone issues with creation date reconstruction using mktime() fallback strategy
- Supports compression algorithm detection for archives created with different pg_dump versions
- Provides warnings for cross-platform compatibility issues (different integer sizes)
- Contains detailed comments about timezone handling limitations and future improvement suggestions
- Version-dependent parsing ensures backward compatibility across PostgreSQL releases
- Compression support validation prevents silent failures when required libraries are missing