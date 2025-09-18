# _discoverArchiveFormat

## Location
src/bin/pg_dump/pg_backup_archiver.c: 2221 - 2354

## Overview
_discoverArchiveFormat is a static function that automatically detects the format of a PostgreSQL archive by examining its contents and structure.

## Definition
```c
static int _discoverArchiveFormat(ArchiveHandle *AH)
```

## Detailed Description
This function performs archive format detection by examining file signatures, directory structures, and header patterns. It supports detection of custom format archives (PGDMP signature), directory format archives (by checking for toc.dat files), tar format archives (by validating tar headers), and identifies text format dumps to provide appropriate error messages. The function sets up a lookahead buffer to cache initial file content for subsequent processing and handles various compression formats including gzip, LZ4, and Zstandard.

## Parameters / Member Variables
- `AH`: ArchiveHandle pointer - the archive handle containing file specification and format information to be determined

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_debug
  - pg_malloc0
  - _fileExistsInDirectory
  - isValidTarHeader
  - S_ISDIR
  - archDirectory, archCustom, archTar (format constants)
  - TEXT_DUMP_HEADER, TEXT_DUMPALL_HEADER (header constants)
  - PG_BINARY_R, READ_ERROR_EXIT (I/O macros)
- Called from (representative examples):
  - _allocAH

## Notes and Other Information
- Static function, only accessible within pg_backup_archiver.c
- Sets up a 512-byte lookahead buffer that can be used by subsequent operations
- Handles both file-based and stdin input sources
- Supports detection of compressed TOC files in directory format (.gz, .lz4, .zst)
- Provides specific error messages for text format dumps suggesting use of psql
- Critical for proper initialization of archive handlers in pg_restore operations
- Returns format identifier that determines which archive-specific functions are used