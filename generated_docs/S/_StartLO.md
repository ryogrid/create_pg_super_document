# _StartLO

## Location
[src/bin/pg_dump/pg_backup_custom.c:371-390](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_custom.c#L371-L390)

## Overview
Initializes the processing of an individual Binary Large Object (BLOB) by recording its OID and setting up compression for the large object data that will follow.

## Definition

```c
static void
_StartLO(ArchiveHandle *AH, TocEntry *te, Oid oid)
```
## Detailed Description
The  function is a mandatory component of the custom archive format that handles the initialization of individual Binary Large Object processing within a BLOB data section. This function is called by the archiver when the dumper calls , and it serves as the granular counterpart to  which initializes the entire BLOB section.

The function performs several critical tasks for each large object:

1. **OID Validation**: Verifies that the provided OID is valid (non-zero), failing with a fatal error if invalid
2. **OID Recording**: Writes the large object's OID to the archive stream for identification during restoration
3. **Compression Setup**: Initializes a new compressor instance specifically for this large object's data

Unlike table data processing, each large object gets its own compression context, allowing for optimal compression of individual large objects and providing clear boundaries between different large objects in the archive.

## Parameters / Member Variables
- `*AH`: Archive handle containing the overall archive state and configuration
- `*te`: Table of Contents entry representing the large object being processed
- `oid`: Object Identifier (OID) of the large object to be dumped
## Dependencies
- Functions called/Symbols referenced:
  -  - Reports fatal error and terminates program (for invalid OID)
  -  - Writes the large object OID to the archive stream
  -  - Initializes compression for the large object data
  -  - Write function for compressed data
- Data structures used:
  -  - Local context for custom format containing compression state
  -  - General table of contents entry
  -  - PostgreSQL object identifier type
- Called from:
  -  - Custom format initialization (function pointer assignment)
  -  - Directory format initialization
  -  - Null format initialization

## Notes and Other Information
- This function is marked as mandatory in the pg_dump architecture
- Each large object gets its own compression context, unlike table data which shares compression across the entire table
- The OID must be preserved exactly for successful restoration of the large object
- Fatal error handling ensures that invalid large objects cannot corrupt the archive
- Works within the context established by  for the overall BLOB section
- Part of the pluggable archive format system enabling different storage backends
- The compression setup allows for optimal handling of potentially very large binary data
- Forms part of the large object processing pipeline:  →  →  →

## Simplified Source

```c
static void
_StartLO(ArchiveHandle *AH, TocEntry *te, Oid oid)
{
    lclContext *ctx = (lclContext *) AH->formatData;

    // Validate OID - must be non-zero for large objects
    if (oid == 0)
        pg_fatal("invalid OID for large object");

    // Write the large object OID for restoration identification
    WriteInt(AH, oid);

    // Initialize compression for this specific large object
    ctx->cs = AllocateCompressor(AH->compression_spec, NULL, _CustomWriteFunc);
}
```