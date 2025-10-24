# WriteHead

## Location
[src/bin/pg_dump/pg_backup_archiver.c:3951-3976](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L3951-L3976)

## Overview
Writes the file header for a custom-format archive in pg_dump, containing magic code, version information, metadata, and timestamps.

## Definition

```c
void
WriteHead(ArchiveHandle *AH)
```
## Detailed Description
This function creates the standard header for PostgreSQL custom-format archive files. The header contains critical metadata needed for archive identification and proper restoration:

1. **Magic signature**: "PGDMP" identifies the file as a PostgreSQL dump
2. **Version information**: Major, minor, and revision numbers for format compatibility
3. **Architecture details**: Integer size and offset size for cross-platform compatibility
4. **Compression info**: Algorithm specification for compressed archives
5. **Timestamp**: Complete creation date/time broken down into individual components
6. **Database metadata**: Original database name, PostgreSQL server version, and pg_dump version

The header format is standardized to ensure archive files can be correctly identified and processed by compatible versions of pg_restore and other PostgreSQL tools.

## Parameters / Member Variables
- `*AH`: ArchiveHandle pointer containing archive state, version info, creation metadata, and database connection details

## Dependencies
- Functions called/Symbols referenced:
  - ARCHIVE_MAJOR, ARCHIVE_MINOR, ARCHIVE_REV (version extraction macros)
  - WriteBufPtr, WriteBytePtr (low-level binary writing functions)
  - [WriteInt](WriteInt.md), WriteStr (typed data writing functions)
  - [PQdb](../P/PQdb.md) (database name extraction from connection)
  - localtime (timestamp conversion)
  - struct tm (time structure)
- Called from:
  - [_CloseArchive](../C/_CloseArchive.md) (in custom, directory, and tar backup formats)

## Notes and Other Information
- Function is non-static and used across multiple backup format implementations
- Uses platform-specific architecture information (intSize, offSize) for cross-platform compatibility
- Stores complete timestamp information allowing for precise archive creation time tracking
- Magic code "PGDMP" serves as file format identifier for PostgreSQL dump files
- Header format must remain backward-compatible across PostgreSQL versions
- Compression algorithm is stored as part of header metadata for proper decompression during restore
- Database connection information is preserved to maintain restore context

## Simplified Source

```c
void WriteHead(ArchiveHandle *AH) {
    struct tm crtm;

    // Write magic signature to identify PostgreSQL dump file
    AH->WriteBufPtr(AH, "PGDMP", 5);

    // Write version information for format compatibility
    AH->WriteBytePtr(AH, ARCHIVE_MAJOR(AH->version));
    AH->WriteBytePtr(AH, ARCHIVE_MINOR(AH->version));
    AH->WriteBytePtr(AH, ARCHIVE_REV(AH->version));

    // Write architecture-specific information
    AH->WriteBytePtr(AH, AH->intSize);
    AH->WriteBytePtr(AH, AH->offSize);
    AH->WriteBytePtr(AH, AH->format);
    AH->WriteBytePtr(AH, AH->compression_spec.algorithm);

    // Write creation timestamp components
    crtm = *localtime(&AH->createDate);
    WriteInt(AH, crtm.tm_sec);
    WriteInt(AH, crtm.tm_min);
    WriteInt(AH, crtm.tm_hour);
    WriteInt(AH, crtm.tm_mday);
    WriteInt(AH, crtm.tm_mon);
    WriteInt(AH, crtm.tm_year);
    WriteInt(AH, crtm.tm_isdst);

    // Write database and version information
    WriteStr(AH, PQdb(AH->connection));        // Database name
    WriteStr(AH, AH->public.remoteVersionStr); // Server version
    WriteStr(AH, PG_VERSION);                  // pg_dump version
}
```