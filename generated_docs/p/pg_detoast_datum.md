# pg_detoast_datum

## Location
[src/backend/utils/fmgr/fmgr.c:1832-1840](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1832-L1840)

## Overview
pg_detoast_datum is a utility function that decompresses or reconstructs PostgreSQL TOAST (The Oversized-Attribute Storage Technique) data when needed, returning the original uncompressed data.

## Definition
```c
struct varlena *pg_detoast_datum(struct varlena *datum)
```

## Detailed Description
pg_detoast_datum is a core function in PostgreSQL's TOAST system that handles the decompression and reconstruction of large or compressed data values. TOAST is PostgreSQL's mechanism for storing large variable-length data that exceeds the page size limit. When data is TOASTed, it may be compressed, stored externally, or both.

The function checks whether the input datum is in an extended (TOASTed) format using the VARATT_IS_EXTENDED macro. If the data is extended, it calls detoast_attr() to perform the actual decompression/reconstruction work. If the data is not extended (i.e., it's stored inline and uncompressed), the function simply returns the original datum pointer unchanged.

This function is essential for PostgreSQL's transparent handling of large data values, allowing the system to work with variable-length data regardless of whether it has been TOASTed or not.

## Parameters / Member Variables
- `datum`: A pointer to a varlena structure that may contain TOASTed (compressed/external) data

## Dependencies
- Functions called/Symbols referenced:
  - VARATT_IS_EXTENDED (macro to check if data is TOASTed)
  - [detoast_attr](../d/detoast_attr.md) (performs the actual decompression/reconstruction)
  - [varlena](../v/varlena.md) (variable-length data structure type)
- Called from (representative examples):
  - PG_DETOAST_DATUM (macro for convenient TOAST handling in functions)
  - PG_ARGISNULL (macro for checking null arguments with TOAST support)

## Notes and Other Information
- Part of PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) system
- Provides transparent access to both TOASTed and non-TOASTed data
- The function is designed to be safe to call on any varlena datum regardless of its TOAST status
- Returns a pointer that may be the same as the input (for non-TOASTed data) or different (for TOASTed data)
- The returned data should be treated as read-only in most contexts
- Critical for PostgreSQL's ability to handle large text, bytea, and other variable-length data types efficiently
- Memory management of the returned pointer depends on whether detoasting occurred