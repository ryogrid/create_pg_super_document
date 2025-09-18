# ReadToc

## Location
src/bin/pg_dump/pg_backup_archiver.c: 2649 - 2820

## Overview
Reads and reconstructs the Table of Contents from an archive file, creating the in-memory TOC structure needed for restore operations.

## Definition
```c
void ReadToc(ArchiveHandle *AH)
```

## Detailed Description
This function deserializes the Table of Contents from an archive file and builds the complete in-memory representation of all database objects contained in the dump. It handles version compatibility by reading different fields based on the archive format version, ensuring that archives created by older versions of pg_dump can still be read. The function processes dependencies, handles backward compatibility for section classifications, and performs immediate processing for special entries like encoding settings. Each TOC entry is linked into a circular linked list for efficient traversal during restore operations.

## Parameters / Member Variables
- `AH`: Archive handle that will be populated with the TOC structure and metadata from the archive

## Dependencies
- Functions called/Symbols referenced:
  - [TocEntry](../T/TocEntry.md), DumpId (struct types)
  - [ReadInt](ReadInt.md), ReadStr (archive reading functions)
  - pg_malloc0, pg_malloc, pg_realloc (memory management)
  - pg_log_warning, pg_log_debug (logging functions)
  - [processEncodingEntry](../p/processEncodingEntry.md), processStdStringsEntry, processSearchPathEntry (special entry processors)
  - Version constants (K_VERS_1_3, K_VERS_1_5, etc.)
  - Section constants (SECTION_NONE, SECTION_DATA, SECTION_POST_DATA, SECTION_PRE_DATA)
- Called from (representative examples):
  - [InitArchiveFmt_Custom](../I/InitArchiveFmt_Custom.md)
  - [InitArchiveFmt_Directory](../I/InitArchiveFmt_Directory.md)

## Notes and Other Information
- Handles version compatibility across multiple archive format versions (1.3 through 1.16+)
- For pre-8.4 archives, manually classifies entries into sections based on description strings
- Dynamically allocates and resizes dependency arrays as needed
- Maintains maxDumpId to track the highest dump ID encountered
- Links all entries into a circular doubly-linked list for efficient navigation
- Performs sanity checking on dump IDs to detect corrupt archives
- Issues warnings for deprecated features like tables WITH OIDS
- Immediately processes special configuration entries (ENCODING, STDSTRINGS, SEARCHPATH)
- The function is critical for restore operations as it builds the complete object dependency graph
- Dependencies are stored as arrays of DumpId values, terminated by NULL in the serialized format
- Each entry includes comprehensive metadata needed for restoration: SQL statements, ownership, tablespace, etc.